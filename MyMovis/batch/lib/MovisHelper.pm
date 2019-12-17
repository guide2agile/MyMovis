#!/usr/bin/perl

package MovisHelper;

=head1 NAME

MovisHelper - Various supporting subroutines for Movis project

=head1 SYNOPSIS

Use in other scripts as utility soubroutines

=cut

=head1 DESCRIPTION

  Various supporting subroutines
 
=cut

=head1 AUTHOR

  guide2agile   https://github.com/guide2agile/MyMovis

=cut

=head1 LICENSE AND COPYRIGHT

  This app is under Apache 2.0 license, see https://www.apache.org/licenses/

  If you use this program you must verify that you are compliant with the IMDb
 terms & conditions. This implementation bases on the specifications published
 on https://www.imdb.com/interfaces/ - accessed December 13th, 2019.

=cut

use strict;
use warnings;
use Exporter 'import';
use Carp;
use Cwd qw(getcwd abs_path);
use DateTime;
use File::Basename;
use File::Copy;
use File::Slurp;
use File::Which;
use Getopt::Long qw(GetOptionsFromArray :config ignore_case_always auto_version auto_help);
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use Log::Log4perl;
use LWP::UserAgent;
use Pod::Usage;
use Readonly;
use String::Format;
use Text::ParseWords;
use YAML::AppConfig;

our $VERSION = 0.1;
our @EXPORT    = qw(inform inform_start inform_end get_logger read_parameters read_configfile fileparse abs_path clean_dir getcwd check_and_adapt_path file_download_unzip %download_file_comment $app_configfile $download_source_uri $app_data_dir $log_configfile @download_file_list $loggername $DEBUG $INFO $WARN $ERROR $FATAL);
our @EXPORT_OK = qw(start_logging);

#### error messages the system uses
#
# agreement on exit_codes (e.g. 0..99 -> INFO, 100..399 -> WARN etc)
Readonly my $INFO_LEVELS  =>   1;   # i.e.   1.. 99
Readonly my $WARN_LEVELS  => 200;   # i.e. 200..499
Readonly my $ERROR_LEVELS => 500;   # i.e. 500..799
Readonly my $FATAL_LEVELS => 800;   # i.e. 800..999
Readonly my $HUNDREDS     => 100;   # to identify the Levels from a code
#
# user & configuration warnings
Readonly my $WAR_181 => 181; # file is not a .gz file '$1'
Readonly my $WAR_182 => 182; # working with yaml config '$1' failed, continuing without
#
# user & configuration errors
Readonly my $ERR_481 => 481; # archive '$1' does not exist and could not be created $2
Readonly my $ERR_482 => 482; # command line parameters: $1 there is an error in the parameter(s). Use '$2 --help' for more information
Readonly my $ERR_483 => 483; # logging configuration file '$1' not found, processing aborted
Readonly my $ERR_484 => 484; # inexistent file '$1'
Readonly my $ERR_485 => 485; # optionfile '$1' not found
Readonly my $ERR_486 => 486; # parameters '$1', '$2' is not understood, use '$2 --help' for more information
Readonly my $ERR_487 => 487; # unknown parameter(s) '$1'
Readonly my $ERR_488 => 488; # unuseable directory '$1'
#
# technical problems
Readonly my $ERR_681 => 681; # gunzip failed: '$1'
Readonly my $ERR_682 => 682; # not able to archive .gz file '$1'
Readonly my $ERR_683 => 683; # parameter error in '$1': '$2'
Readonly my $ERR_999 => 999; # general fatal error

#### default values, some might get overwritten by config-file or commandline
# logging, debug & config defaults
Readonly my $DEFAULT_PAR_OPT_QUIET   => 0;
Readonly my $DEFAULT_PAR_OPT_VERBOSE => 0;
Readonly my $DEFAULT_LOG_WATCH_SEC   => 60;
Readonly my $DEFAULT_LOGGER          => 'myMovis';
Readonly my $DEFAULT_LOGLEVEL        => undef;
Readonly my $DEFAULT_LOG_CONFIGFILE  => '../config/mymovis_logging.config';
Readonly my $DEFAULT_APP_CONFIGFILE     => '../config/mymovis_config.yaml';
#
# application data defaults
Readonly my $DEFAULT_IMDB_TARGET_DIR => '../data/imdb/';
Readonly my $DEFAULT_IMDB_SOURCE     => 'https://datasets.imdbws.com/';
Readonly my @DEFAULT_IMDB_FILE_LIST  => ('title.akas.tsv.gz', 'title.basics.tsv.gz', 'title.crew.tsv.gz', 'title.episode.tsv.gz',
                                         'title.principals.tsv.gz', 'title.ratings.tsv.gz', 'name.basics.tsv.gz', );

### various constant declarations
# numeric constants
Readonly my $MINIMAL_DOWNLOAD_SIZE => 1_000_000;
Readonly my $USAGE_SELECT_SECTIONS => 99;
Readonly my $OS_DIR_SEP            => File::Spec->catfile( q{}, q{} );
#
# log levels
Readonly our $DEBUG                 => 'DEBUG';
Readonly our $INFO                  => 'INFO';
Readonly our $WARN                  => 'WARN';
Readonly our $ERROR                 => 'ERROR';
Readonly our $FATAL                 => 'FATAL';

#### global variable definitions
# statistical data for the inform system
our $start_time  = DateTime->now();
our $debug_count = 0;
our $info_count  = 0;
our $warn_count  = 0;
our $max_code    = 0;
#
# effective values used (based on defaults -> config file -> commandline)
our $app_configfile    = $DEFAULT_APP_CONFIGFILE;
our $log_configfile    = $DEFAULT_LOG_CONFIGFILE;
our $loggername        = $DEFAULT_LOGGER;
our $log_level         = $DEFAULT_LOGLEVEL;
our $log_watch_seconds = $DEFAULT_LOG_WATCH_SEC;
our $opt_quiet         = $DEFAULT_PAR_OPT_QUIET;
our $opt_verbose       = $DEFAULT_PAR_OPT_VERBOSE;
our $opt_dump = 0;
our %download_file_comment;
our @download_file_list;
our $download_source_uri;
our $app_data_dir;

#HTTP::Request->new( 'GET', 'https://datasets.imdbws.com/title.ratings.tsv.gz' );

=head2 inform

Informs on console and in logfile(s) about status, if code != 0 exits

=cut
sub inform {
    my $par_level    = shift;
    my $par_logger   = shift;
    my $par_code     = shift;
    my $par_text     = shift;
    my @par_textvals = @_;
#TODO Same Functionality as inform_end if crashing, but without the exit ;-)
    # if variables %1 %2 etc in the $par_text then substitute with values from list
    if (@par_textvals) {
       my $counter = 1;
       foreach my $value (@par_textvals) {
          $par_text =~ s/\%$counter/$value/gsxm;
          $counter++;
       }
    }
    
    # make sure we have no undef parameter values
    if (!defined($par_level)) {$par_level = $FATAL}
    if (!defined($par_code))  {$par_code = $ERR_999}
    if (!defined($par_text))  {$par_text = 'fatal program error, missing errormessage, processing ended'}

    my $ret_val;

    #if no logger given, try to get the default logger;
    if (!defined($par_logger)) {
       $par_logger = get_logger();
    }

    # adapt console text (e.g. starts uppercase)
    my $console_text = $par_text . ".";
    $console_text =~ /^(.)(.*)/sxm;
    $console_text = (uc $1) . $2; 
    my $errorlevel;
    {  # this block is for integer division
       use integer;
       $errorlevel = $par_code / $HUNDREDS; 
    }

    # while multiline and extra spaces might be good for display - they are removed for the logfile
    $par_text =~ s/^\s*//sxm;
    $par_text =~ s/\s*$//sxm;
    $par_text =~ s/\r/\ \|\ /gsxm;
    $par_text =~ s/\n/\ \|\ /gsxm;
    $par_text =~ s/\ \|\ \ \|\ /\ \|\ /gsxm;
    $par_text =~ s/\s{2,}/\ /gsxm;

    # DEBUG only goes to logfile
    if ($par_level eq $DEBUG) {
       if ($par_code > $max_code) {$max_code = $par_code};
       $debug_count++;
       if ($par_code) {
          $par_text = "[DEBUG #$par_code] " . $par_text;
       }
       if ($par_logger) {
          $par_logger->debug($par_text)
       }
       return;
    }
    
    # INFO goes to screen if verbose and goes to logfile, 
    if ($par_level eq $INFO) {
       if ($par_code > $max_code) {$max_code = $par_code};
       $info_count++;
       if (!$opt_quiet && $opt_verbose) {
          $ret_val = print $console_text . "\n";
       } else {
          $ret_val = 1;
       }
       if ($par_code) {
          $par_text = "[INFO #$par_code] " . $par_text;
       }
       if ($par_logger) {
          $par_logger->info($par_text)
       }
       # return only on success, otherwise continue reporting the printing error
       if ($ret_val) {
          return;
       } else {
          $par_level = $FATAL;
          $par_text .= '*** UNHANDLED FATAL ERROR *** Printing to console failed!';
          $par_code  = $ERR_999;
          $errorlevel = 9;
       }
    }
    
    # WARN goes to screen and goes to logfile, 
    if ($par_level eq $WARN) {
       if ($par_code > $max_code) {$max_code = $par_code};
       $warn_count++;
       if ($par_code) {
          $console_text = "== WARNING #$par_code -  $console_text";
       } else {
          $console_text = "== WARNING -  $console_text";
       }        
       $ret_val = print {*STDERR} $console_text . "\n";
       if ($par_code) {
          $par_text = "[WARN #$par_code] " . $par_text;
       }
       if ($par_logger) {
          $par_logger->warn($par_text)
       }
       # return only on success, otherwise continue reporting the printing error
       if ($ret_val) {
          return;
       } else {
          $par_level = $FATAL;
          $par_text .= '*** UNHANDLED FATAL ERROR *** Printing to console failed!';
          $par_code  = $ERR_999;
          $errorlevel = 9;
       }
    }
    
    # ERROR goes to logfile and exits dying to screen
    if ($par_level eq $ERROR) {
       $! = $errorlevel;
       $console_text = "### ERROR #$par_code -  $console_text";
       $ret_val = print {*STDERR} $console_text . "\n";
       if ($par_code) {
          $par_text = "[ERROR #$par_code] " . $par_text;
       }
       if ($par_logger) {
          $par_logger->error($par_text)
       }
       # die only on success, otherwise continue reporting the printing error
       if ($ret_val) {
          if ($opt_dump) {
             $! = 
             confess ("*** Scrippt stopped!\n");
          } else {
             $! = 
             die ("*** Scrippt stopped!\n");
          }
       } else {
          $par_level = $FATAL;
          $par_text .= '*** UNHANDLED FATAL *** Big problems, even printing to console failed!';
          $par_code  = $ERR_999;
          $errorlevel = 9;
       }
    }

    # FATAL goes to logfile and exits dying to screen
    if ($par_level eq $FATAL) {
       $! = $errorlevel;
       if ($par_code) {
          $par_text = "[FATAL #$par_code] " . $par_text;
       }
       if ($par_logger) {
          $par_logger->logconfess($par_text);
       }
       confess($par_text);
    }
    
    # if we got here, we have a problem. We should have exited or died above
    if ($par_logger) {
       $par_logger->fatal("   *** $par_level *** UNHANDLED INFORM CODE #$par_code *** $par_text");
    }
    confess ("   *** $par_level *** UNHANDLED INFORM CODE #$par_code *** $par_text"); 
}

=head2 start_logging

Prepare logging based on global logging configuration file.

=cut
sub start_logging {

    # check if requested configuration file can be used
    if ( !-f $log_configfile ) {
        inform $ERROR, undef, $ERR_483, "logging configuration file '%1' not found, processing aborted",$log_configfile;
    }

    # initialize and configure logging as requested by parameters/defaults
    Log::Log4perl->init_and_watch( $log_configfile, $log_watch_seconds );
    $Log::Log4perl::caller_depth = 1;
    my $logger = Log::Log4perl->get_logger($loggername);
    if ($log_level) { $logger->level($log_level) }

    # log event that logging is active now
    inform $DEBUG, $logger, 0, "logging started: config '%1', logger '%2', read config every %3 seconds",
                                $log_configfile, $loggername,$log_watch_seconds;
    # done initializing, return the logger constructed
    return $logger;
}

=head2 get_logger

Return logger for the given 

=cut
sub get_logger {
    my $par_loggername = shift // $loggername;

    my $logger = Log::Log4perl->get_logger($par_loggername);
    return $logger;
}

=head2 inform_end

Reports completion of the script setting exitstate to the value given

=cut
sub inform_end {
    my $par_logger = shift // undef;
    my $par_exit   = shift // undef;

    # note $end_time and calculate how long the script ran
    my $end_time        = DateTime->now();
    my $end_time_string = $end_time->datetime(q{ });
    my $elapsed_time    = $end_time->epoch() - $start_time->epoch();
    if (!defined($par_logger)) {
       $par_logger = Log::Log4perl->get_logger($loggername);
    }
    if (!defined($par_exit)) {
       $par_exit = $max_code / $HUNDREDS; 
    }
    if (defined($par_logger)) {
       inform $INFO, $par_logger, 1, "script had %1 debug, %2 info, %3 warn messages logged", $debug_count, $info_count, $warn_count;
       inform $INFO, $par_logger, 1, "script completed at %1 after %2 seconds", $end_time_string, $elapsed_time;
    }
    exit $par_exit;
}

=head2 inform_start

Reports start of the script setting exitstate to the value given

=cut
sub inform_start {

    # initialize logging and note start of application
    my $logger = start_logging();
    my $start_time_string = $start_time->datetime(q{ });
    inform $INFO, $logger, 0, "Script '%1' started at %2", clean_dir($0), $start_time_string;
    return $logger;
}

=head2 clean_dir

Turns directory separating slashes or backslashes into correct one for the OS.

=cut
sub clean_dir {
    my $dir_to_clean = shift;

    $dir_to_clean =~ s/\//$OS_DIR_SEP/gsxm;
    $dir_to_clean =~ s/\\/$OS_DIR_SEP/gsxm;

    return $dir_to_clean;
}

=head2 read_parameters

  Reading like command line parameters, but from the array provided.

=cut
sub read_parameters {
    my $parameter_ref = shift;

    # if we are not told what we read from, we read from ARGV
    if (!defined($parameter_ref)) {
       $parameter_ref = \@ARGV;
    }
    
    # process options
    my @parameters_in_use = @$parameter_ref;
    my $parameter_string  = join q{ }, @parameters_in_use;
    my $return_status     = GetOptionsFromArray(
        $parameter_ref,

        # data configuration
        'files=s'          => \@download_file_list,
        'comments=s'       => \my @local_file_comment,
        'source=s'         => \$download_source_uri,
        'target=s'         => \$app_data_dir,
        'appconfig|yaml=s' => \$app_configfile,
        'logconfig=s'      => \$log_configfile,
        'logger=s'         => \$loggername,
        'loglevel=s'       => \$log_level,
        'logwatch|watch=i' => \$log_watch_seconds,
        'debug'            => \my $local_opt_debug,
        'dump'             => \$opt_dump,
        'info'             => \my $local_opt_info,
        'quiet!'           => \$opt_quiet,
        'verbose!'         => \$opt_verbose,
        'manual'           => \my $local_opt_manual,
        'optionfile=s'     => \my $local_option_file,
    );
    if ( !$return_status ) {
        inform $ERROR, undef, $ERR_482, "command line parameters:\n" 
                                 . "$parameter_string\n"
                                 . "there is an error in the parameter(s),\n"
                                 . "use '" . basename($0) . " --help' for "
                                 . "more information";
    }
    if (@$parameter_ref) {

        # parameter list not empty means unprocessed content remained
        inform $ERROR, undef, $ERR_486, "parameters '@parameters_in_use', "
          . "'@$parameter_ref' is not understood, use '" . basename($0) 
          . " --help' for more information";
    }

    if ($local_opt_manual) {
        pod2usage(
            -verbose  => $USAGE_SELECT_SECTIONS,
            -sections => "NAME|DESCRIPTION|SYNOPSIS|OPTIONS|AUTHOR"
        );
    }

    # store file comments received in the corresponding hash
    if (@local_file_comment) {
        my $file_count = 0;
        my $one_comment;
        foreach my $one_file (@download_file_list) {
            if ( defined( $local_file_comment[$file_count] ) ) {
                $one_comment = $local_file_comment[$file_count];
            }
            $one_file =~ s/[.]gz$//sxm;
            $download_file_comment{$one_file} = $one_comment;
            $file_count++;
        }
    }

    # logging options that are shortcuts for log_levels
    if ($local_opt_info) {
        $log_level = 'INFO';
    }
    if ($local_opt_debug) {
        $log_level = 'DEBUG';
    }

    # process if option-file specified
    if ($local_option_file) {
        if ( !-f $local_option_file ) {

            # optionfile does not exist/is not accessible
            inform $ERROR, undef, $ERR_485, "optionfile '$local_option_file' not found";
        }

        # re-read parameters from file
        my @options = read_file( $local_option_file, chomp => 1 );

        # new backup copy of parameters, are changed by options file
        my $parameter_list = join q{ }, @options;
        $parameter_list =~ s/\r/ /gsxm;
        $parameter_list =~ s/\n/ /gsxm;
        $parameter_list =~ s/\ \ /\ /gsxm;
        my @local_parameters = shellwords($parameter_list);

        # repeat the analysis with
        read_parameters( \@local_parameters );
    }
}

=head2 read_configfile

Readig data specifications from yaml file.
  
=cut
sub read_configfile {

    my $logger = Log::Log4perl->get_logger($loggername);

    # read configuration file to object yaml_config
    my $yaml_config = YAML::AppConfig->new( file => $app_configfile );
    if ( !defined($yaml_config) ) {

        # inform, set defaults and return
        inform $WARN, $logger, $WAR_182, "working with yaml config "
                             . "'$app_configfile' failed, continuing without";
        if ( !defined($app_data_dir) ) {
            $app_data_dir = $DEFAULT_IMDB_TARGET_DIR;
        }
        if ( !defined($download_source_uri) ) {
            $download_source_uri = $DEFAULT_IMDB_SOURCE;
        }

        if (@download_file_list) {
            @download_file_list = @DEFAULT_IMDB_FILE_LIST;
        }
        # file not found means nothing is left to do -> return
        return;
    }
    if ( !defined($app_data_dir) ) {
        if ( defined( $yaml_config->get_IMDB()->{'directory'} ) ) {
            $app_data_dir = $yaml_config->get_IMDB()->{'directory'};
        }
        else {
            $app_data_dir = $DEFAULT_IMDB_TARGET_DIR;
        }
    }

    if ( !defined($download_source_uri) ) {
        if ( defined( $yaml_config->get_IMDB()->{'download'} ) ) {
            $download_source_uri = $yaml_config->get_IMDB()->{'download'};
        }
        else {
            $download_source_uri = $DEFAULT_IMDB_SOURCE;
        }
    }

    # get the filenames only, if the list is empty
    my $get_filenames = 1;
    if (@download_file_list) {
        $get_filenames = 0;
    }
    my $counter = 0;
    while (
        defined( my $files = $yaml_config->get_IMDB()->{'files'}[$counter] ) )
    {
        my $file_name =
          $yaml_config->get_IMDB()->{'files'}[$counter]{'filename'};
        if ($get_filenames) {
            push @download_file_list, $file_name;
        }
        $download_file_comment{$file_name} =
          $yaml_config->get_IMDB()->{'files'}[$counter]{'comment'};
        $counter++;
    }
    # make the config and directory path absolute
    inform $DEBUG, $logger, 0, "locations - appconfig: $app_configfile, "
                          . "logconfig: $log_configfile, target: $app_data_dir";

    ( my $scriptname, my $script_directory, my $script_extension ) =
      fileparse( $0, '\.[^.]*$' );
    ( my $appconfig, my $appconfig_dir, my $appconfig_ext ) =
      fileparse( $app_configfile, '\.[^.]*$' );
    ( my $logconfig, my $logconfig_dir, my $logconfig_ext ) =
      fileparse( $log_configfile, '\.[^.]*$' );
    my $current_directory = abs_path( getcwd() ) . '/';
    $app_data_dir = clean_dir(
        check_and_adapt_path(
            $app_data_dir, [ $current_directory, $script_directory ]
        )
    );
    my $script_directory_list =
      [ $script_directory, $current_directory, $app_data_dir, ];
    my $current_directory_list =
      [ $current_directory, $script_directory, $app_data_dir, ];
    $appconfig_dir = check_and_adapt_path( $appconfig_dir, $script_directory_list,
        $appconfig . $appconfig_ext );
    $app_configfile = clean_dir( $appconfig_dir . $appconfig . $appconfig_ext );
    $logconfig_dir   = check_and_adapt_path( $logconfig_dir, $script_directory_list,
        $logconfig . $logconfig_ext );
    $log_configfile = clean_dir( $logconfig_dir . $logconfig . $logconfig_ext );
    
    inform $DEBUG, $logger, 0, "absolute - appconfig: $app_configfile, "
                         . "logconfig: $log_configfile, target: $app_data_dir";

}

=head2 gunzip_and_archive

gunzip .gz file in directory provided, archive .gz in .\archive of directory.

=cut
sub gunzip_and_archive {
    my $gz_file   = shift;
    my $directory = shift;

    # start logging
    my $logger = Log::Log4perl->get_logger($loggername);

    # check parameters of call
    inform $DEBUG, $logger, 0, "sub unzip_and_archive called for file: "
                             . "$gz_file directory: $directory";
    if ( ( !defined($gz_file) ) || ( !defined($directory) ) ) {
        my $error_string;
        if ( !defined($gz_file) ) {
            $error_string .= " $gz_file";
        }
        if ( !defined($directory) ) {
            $error_string .= " $directory";
        }
        inform $ERROR, $logger, $ERR_487, "unknown parameter(s) '$error_string'";
    }
    if ( !-d $directory ) {
        inform $ERROR, $logger, $ERR_488, "unuseable directory '$directory'";
    }
    if ( !-f $directory . $gz_file ) {
        inform $ERROR, $logger, $ERR_484, "inexistent file '$directory$gz_file'";
    }
    if ( !( $gz_file =~ /[.]gz$/isxm ) ) {
        inform $WARN, $WAR_181, "file is not a .gz file '$directory$gz_file'";
    }

    my $unzipped_file = $gz_file;

    # name of unzipped file will be same as filename provided without .gz
    $unzipped_file =~ s/[.]gz$//sxm;
    if ( !-d "$directory/archive/" ) {

        # if the archive folder is missing, try to create it
        if ( !mkdir "$directory/archive/" ) {
            inform $ERROR, $logger, $ERR_481, "archive '$app_data_dir/archive/'"
                              . " does not exist and could not be created $!";
        }
        else {
            inform $INFO, $logger, 0, "mkdir '$app_data_dir/archive/' "
                                    . "- archive folder created";
        }
    }
    inform $INFO, $logger, 0, "gunzip      '%1' => '%2' (in '%3')",$gz_file, $unzipped_file, $directory;
#    inform $INFO, $logger, 0, "gunzip      '$gz_file' => "
#                            . "'$unzipped_file' (in $directory)";
    gunzip "$directory$gz_file" => "$directory$unzipped_file"
      or inform $ERROR, $logger, $ERR_681, "gunzip failed: '$GunzipError'";
    inform $INFO, $logger, 0, "archiving   '%1' to '%2'", $gz_file,clean_dir("$directory/archive/$gz_file");
#    inform $INFO, $logger, 0, "archiving   '$gz_file' to " 
#                            . clean_dir("$directory/archive/$gz_file");
    if ( !( move( "$directory$gz_file", "$directory" . "archive/$gz_file" ) ) )
    {
        inform $ERROR, $logger, $ERR_682, "not able to archive .gz file '$directory$gz_file'";
    }
}

=head2 file_download_unzip

download file from source and store it in target directory.

=cut

sub file_download_unzip {
    my $source        = shift;
    my $download_file = shift;
    my $target_dir    = clean_dir(shift);
    my $file_comment  = shift // q{};

    my $logger = Log::Log4perl->get_logger($loggername);
    if (   !defined($source)
        || !defined($download_file)
        || !defined($target_dir) )
    {
        my $error_string;
        if ( !defined($source) ) {
            $error_string .= " $source";
        }
        if ( !defined($download_file) ) {
            $error_string .= " $download_file";
        }
        if ( !defined($target_dir) ) {
            $error_string .= " $target_dir";
        }

        inform $ERROR, $logger, $ERR_683, "parameter error in 'file_download_unzip':"
                                   . " '$error_string'";
    }
    inform $DEBUG, $logger, 0, "source:$source file:$download_file "
                             . "target: $target_dir comment: $file_comment";
    if ( $file_comment ne q{} ) { $file_comment = " - $file_comment" }

    my $sysCommand = "";
    my $return     = "";

    inform $INFO, $logger, 0, "downloading '$source$download_file$file_comment'";
    inform $INFO, $logger, 0, "to target   '$target_dir" . $download_file . "'";

    # download file from source to targetfile
    my $result1 = my $user_agent = LWP::UserAgent->new;
    my $result2 = $user_agent->mirror( "$source$download_file",
        "$target_dir$download_file" );

    #TODO handle download errors?

    # is size of download acceptable (bigger than $MINIMAL_DOWNLOAD_SIZE)
    my $download_size = -s "$target_dir$download_file";
    if ( $download_size < $MINIMAL_DOWNLOAD_SIZE ) {

        # download failed
        inform $INFO, $logger, 0, "download of '$target_dir$download_file' is"
                                . " only $download_size bytes, did it fail?";
    }

    # if we downloaded a .gz file we gunzip it and archive the original .gz
    if ( $download_file =~ /[   .]gz$/isxm ) {
        inform $DEBUG, $logger, 0, "gunzipping file $target_dir$download_file";
        gunzip_and_archive( $download_file, $target_dir );
    }
}

=head2 check_and_adapt_path

Prepare logging based on global logging configuration file.

=cut

sub check_and_adapt_path {
    my $directory_name = shift;
    my @directory_list = shift;
    my $file_name      = shift // undef;

    # if absolute path, just return what you got
    if ( !( $directory_name =~ /^[.]/sxm ) ) {
        return $directory_name;
    }

    if ( defined $file_name ) {

        # a filename has been specified, check to find it
        if ( -f $directory_name . $file_name ) {

            # return absolute path
            ( my $file, my $dir, my $ext ) =
              fileparse( abs_path( $directory_name . $file_name, '\.[^.]*$' ) );
            return $dir;
        }
        foreach my $directory (@directory_list) {
            if ( -f $directory . $file_name ) {

                # return absolute path
                ( my $file, my $dir, my $ext ) =
                  fileparse( abs_path( $directory . $file_name, '\.[^.]*$' ) );
                return $dir;
            }
        }
        if ( -f $file_name ) {

            # make path absolute and return
            ( my $file, my $dir, my $ext ) =
              fileparse( abs_path( $file_name, '\.[^.]*$' ) );
            return $dir;
        }
    }
    else {
        # just a directory
        if ( -d abs_path($directory_name) ) {

            # it exists without furter search
            my $directory_found = abs_path($directory_name);
            if ( !( $directory_found =~ /[\\\/]$/sxm ) ) {
                $directory_found .= '/';
            }
            return $directory_found;
        }
        else {
            foreach my $directory (@directory_list) {
                if ( -d $directory . $directory_name ) {

                    # return absolute path
                    my $directory_found =
                      abs_path( $directory . $directory_name );
                    if ( !( $directory_found =~ /[\\\/]$/sxm ) ) {
                        $directory_found .= '/';
                    }
                    return $directory_found;
                }
            }
        }
    }
    return undef;
}

############ MAIN ############

1;

__END__

