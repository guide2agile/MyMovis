#!/usr/bin/perl

=head1 NAME

IMDb_download - Program to download and unzip offline files from IMDb website

=head1 SYNOPSIS

  IMDb_download [options]
  
    --option <filename>     Read the options from a file, not commandline
    --source <web-page>     What web-page to use
    --target <directory>    Directory to be used as target for the download
    --file <filename>       Name of a .gz to download e.g. title.akas.tsv.gz
                            file parameter can be repeated multiple times
    --quiet | noquiet       No output to standard output
    --verbose | noverbose   Extended processing information on standard output
    --help                  Show this help information
    --manual                Manual, the more elaborate help
    --version               Show version number
    
  Defaults if parameters are omitted are: 
    --source https://datasets.imdbws.com/
    --target ../data/imdb/
    --file title.akas.tsv.gz
    --file title.basics.tsv.gz
    --file title.crew.tsv.gz
    --file title.episode.tsv.gz
    --file title.principals.tsv.gz
    --file title.ratings.tsv.gz
    --file name.basics.tsv.gz
    --noquiet
    --noverbose
=cut

=head1 DESCRIPTION

  Program to download the Internet Movie Database (IMDb) offline files. 
  Downloaded from https://datasets.imdbws.com/
 
  If you use this program you must verify that you are compliant with the IMDb
 terms & conditions. This implementation bases on the specifications published
 on https://www.imdb.com/interfaces/ - accessed December 12th, 2019.
 
  This app is under Apache 2.0 license, see https://www.apache.org/licenses/

=cut

=head1 AUTHOR

  guide2agile
  --
  https://github.com/guide2agile/MyMovis

  $Id: IMDb_download $

=cut

use strict;
use warnings;
use Carp;
use File::Copy;
use Log::Log4perl;
use YAML::AppConfig;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use Getopt::Long 
    qw(GetOptionsFromArray :config ignore_case_always auto_version auto_help);
use Pod::Usage;
use Cwd qw(getcwd abs_path);
use File::Which;
use File::Basename;
use Readonly;
use Text::ParseWords;

our $VERSION = 0.6;

# default values, some might get overwritten by config-file or commandline
# logging & debug defaults
Readonly my $DEFAULT_PAR_OPT_QUIET   => 0;
Readonly my $DEFAULT_PAR_OPT_VERBOSE => 0;
Readonly my $DEFAULT_LOG_WATCH_SEC   => 60;
Readonly my $DEFAULT_LOGGER          => 'myMovis';
Readonly my $DEFAULT_LOGLEVEL        => 'WARN';
Readonly my $DEFAULT_LOG_CONFIGFILE  => '../config/logging.config';
Readonly my $DEFAULT_CONFIG_FILE     => '../config/imdb_config.yaml';
Readonly my $DEFAULT_IMDB_TARGET_DIR => "../data/imdb/";
Readonly my $DEFAULT_IMDB_SOURCE     => "https://datasets.imdbws.com/";
Readonly my @DEFAULT_IMDB_FILE_LIST  => (
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz");

# effective values used
my %imdb_file_comment;
my @imdb_file_list;
my $imdb_source;
my $imdb_target_dir;
my $imdb_configfile   = $DEFAULT_CONFIG_FILE;
my $log_configfile    = $DEFAULT_LOG_CONFIGFILE;
my $log_logger        = $DEFAULT_LOGGER;
my $log_level         = $DEFAULT_LOGLEVEL;
my $log_watch_seconds = $DEFAULT_LOG_WATCH_SEC;
my $opt_quiet         = $DEFAULT_PAR_OPT_QUIET;
my $opt_verbose       = $DEFAULT_PAR_OPT_VERBOSE;

=head2 read_commandline

  Reading command line data.

=cut
sub read_commandline {
    my $parameter_ref = shift;

    # process options
    my @parameters_in_use = @$parameter_ref;
    my $return_status     = GetOptionsFromArray(
        $parameter_ref,

        # data configuration
        'file=s'           => \@imdb_file_list,
        'source=s'         => \$imdb_source,
        'target=s'         => \$imdb_target_dir,
        'appconfig|yaml=s' => \$imdb_configfile,
        'logconfig=s'      => \$log_configfile,
        'logger=s'         => \$log_logger,
        'loglevel=s'       => \$log_level,
        'logwatch|watch=i' => \$log_watch_seconds,
        'debug'            => \my $local_opt_debug,
        'info'             => \my $local_opt_info,
        'quiet!'           => \$opt_quiet,
        'verbose!'         => \$opt_verbose,
        'manual'           => \my $local_opt_manual,
        'optionfile=s'     => \my $local_option_file,
    );
    if ( !$return_status ) {
        print "You specified these command line perameters:\n";
        print join ' ', @parameters_in_use . "\n";
        print "Not able to understand the parameter(s) specified.\n";
        croak "Use '" . basename($0) . " --help' for more information\n"
    }
    if (@$parameter_ref) {
       # parameter list not empty means unprocessed content remained
       croak "You submitted parameters '@parameters_in_use'.\nThe following parameters are not understood '@$parameter_ref', use --help for usage."; 
    }
    
    if ($local_opt_manual) {
       #TODO define sections making up the full content
       pod2usage( -verbose => 99, 
                  -sections => "NAME|DESCRIPTION|OPTIONS|EXAMPLES|AUTHOR" );
       exit;
    }
    
    if ($local_opt_info) {
       $log_level = 'INFO';
    }
    if ($local_opt_debug) {
       $log_level = 'DEBUG';
    }

    if ($local_option_file) {
       if ( !-f $local_option_file ) {
          # optionfile does not exist/is not accessible
          croak "Specified optionfile '$local_option_file' not found!";
       }

       # re-read parameters from file
       my @options = read_file( $local_option_file, chomp => 1 );

       # new backup copy of parameters, are changed by options file
       my $parameter_list = join ' ', @options;
       $parameter_list =~ s/\r/ /gsxm;
       $parameter_list =~ s/\n/ /gsxm;
       $parameter_list =~ s/\ \ /\ /gsxm;
       my @local_parameters = shellwords($parameter_list);

       # repeat the analysis with 
       read_application_parameters( \@local_parameters );
    }
}

=head2 read_config_file

Readig data specifications from yaml file.
  
=cut
sub read_config_file {

#TODO LOGGING    # get logger to enable logging, start logging
#    my $log_loc_logger = Log::Log4perl->get_logger($log_logger);

    # read configuration file to object yaml_config
    my $yaml_config = YAML::AppConfig->new( file => $imdb_configfile );
    if (!defined ($yaml_config)) {
       #TODO no config file warn and continue
       return;
    }
    if (!defined($imdb_target_dir)) {
       if (defined ($yaml_config->get_IMDB()->{'directory'})) {
          $imdb_target_dir = $yaml_config->get_IMDB()->{'directory'};
       } else {
          $imdb_target_dir = $DEFAULT_IMDB_TARGET_DIR;
       }
    }

    if (!defined($imdb_source)) {
       if (defined ($yaml_config->get_IMDB()->{'download'})) {
          $imdb_source = $yaml_config->get_IMDB()->{'download'};
       } else {
          $imdb_source = $DEFAULT_IMDB_SOURCE;
       }
    }

    # load all file definitions from configuration file
    if (!(@imdb_file_list)) {
#TODO FOR-LOOP
       for (
          my $counter = 0 ;
          defined( my $files = $yaml_config->get_IMDB()->{'files'}[$counter] ) ;
          $counter++
          )
       {  
          my $file_name = 
             $yaml_config->get_IMDB()->{'files'}[$counter]{'filename'};
          push @imdb_file_list, $file_name;
          $imdb_file_comment{ $file_name } =
             $yaml_config->get_IMDB()->{'files'}[$counter]{'comment'};
       }
    }

#TODO DUMP CONFIGURATION JUST READ to DEBUG logger
}

sub file_download_unzip {
    my $source = shift;
    my $download_file = shift;
    my $target_dir = shift;
    
    my $sysCommand = "";
    my $return = "";

    if (!$opt_quiet) {
       print "Downloading - " . $source . $download_file . "\n";
    }
    if (!$opt_quiet && $opt_verbose) {
       print "Target      : " . $target_dir . $download_file . "\n";
    }

    if (!(which 'curl')) {
       print {*STDERR} "Warning, perl was not able to find 'curl' utility.\n";
       print {*STDERR} "So download might or might not work. "
                     . "Make sure 'curl' utility is available";
    }
    $sysCommand = "curl -O $source$download_file "
                . "-o $target_dir$download_file --create-dirs -s -S";
    if ($return = `$sysCommand 2>&1`) {
        print "Curl Command : " . $sysCommand . "\n";
        print "Curl Returns : " . $return . "\n";
    	die 'was not able to curl successfully';
    }

    my $unzipped_file = $download_file;
    if (!$opt_quiet && $opt_verbose) {
       print "gunzip      : " . $target_dir . $unzipped_file . "\n";
    }
    $unzipped_file =~ s/\.gz$//;
    if (!-d './archive/') {
       # if the archive folder is missing, create it
       if (!mkdir './archive/') {
          die "\nArchive folder '$imdb_target_dir/archive/' does not exist"
            . " and could not create it $!.";
       } else {
          if (!$opt_quiet && $opt_verbose) {
             print "mkdir       : Archive folder '$imdb_target_dir/archive/'"
                 . " created\n";
          }
       }
    }
    if (!$opt_quiet && $opt_verbose) {
       print "gunzip      : " . $target_dir . $unzipped_file . "\n";
    }
    gunzip $download_file => $unzipped_file 
       or die "gunzip failed: $GunzipError\n";
    if (!$opt_quiet && $opt_verbose) {
       print "archive .gz : to  $target_dir/archive/$download_file\n";
    }
    if (!(move($download_file, './archive/' . $download_file))) {
#          $log_loc_logger->error("specified $param_shortdesc file '$param_filename' "
#                       . "exists and not able to rename it to "
#                       . "'$local_backupname' $!.");
#          report_problem ($param_errornum, "$param_shortdesc '$param_filename'"
#                        . " already exists and not able to rename it to "
#                        . "'$local_backupname' $!.");
       die "\nwas not able to archive original .gz files"
    } 
    if (!$opt_quiet && $opt_verbose) {
       print "\n";
    }
}

sub initialize_logging {
    # configuration loaded, start logging
    if ( !-f $log_configfile ) {
        confess("Logging configuration file '$log_configfile' not"
              . " found, processing aborted!\n" );
    }
    
    Log::Log4perl->init_and_watch( $log_configfile, 
                                   $log_watch_seconds );

#TODO muss das nur einmal gesetzt werden, oder immer wieder beim Logger holen?
    my $local_logger = Log::Log4perl->get_logger($log_logger);
    if ($log_level) {
       $local_logger->level($log_level);
    }
    
    my $log_message = "logging started: config '$log_configfile', "
                    . "logger '$log_logger', read config every "
                    . "$log_watch_seconds seconds";
    if ($log_level) {
       $log_message = $log_message . ", log level '$log_level'"; 
    }
    $local_logger->debug( $log_message );

    return $local_logger;
   
}

############ MAIN ############

#TODO if not quiet report starting time 

read_config_file();
read_commandline(\@ARGV);
my $local_logger = initialize_logging();

my $path_saved = getcwd;
if (!$opt_quiet && $opt_verbose) {
   print "Program started in directory " . abs_path($path_saved) . "\n";
}
chdir $imdb_target_dir;
my $working_path = getcwd;
if (!$opt_quiet && $opt_verbose) {
   print "Switched to directory ".abs_path($working_path)." to work\n\n";
}

# process list of files to download
while(my $imdb_file_to_download = shift(@imdb_file_list)) {
   file_download_unzip ($imdb_source, 
                        $imdb_file_to_download, 
                        $imdb_target_dir,
                        $imdb_file_comment{$imdb_file_to_download});
}

chdir $path_saved;
if (!$opt_quiet && $opt_verbose) {
   print "Program switched directory back to $path_saved\n";
}

#TODO if not quiet report ending time and time used

#TODO Logging
#TOD DO I HAVE TO CLOSE THE LOGGER BEFORE PROGRAM FINISHES

#TODO how can we log if/when we are crashing
#TODO CONFESS ON ERROR
#TODO Logging for errors and fatals
    #TODO include error handling for file not exist or any key not exist
    #TODO confess with error message if any problems occur
