#!/usr/bin/perl
#TODO how can we log if/when we are crashing
#TODO CONFESS ON ERROR
#TODO Logging for errors and fatals

=head1 NAME

IMDb_tsv2csv - Program to prepare IMDb download files for easy DB import

=head1 SYNOPSIS

  IMDb_tsv2csv [options]
  
  Help Options:
    -- help    Show help information.
    -- manual  Read manual, the more elaborate help.
    -- version Show version number.

=cut

=head1 DESCRIPTION

  Program to process Internet Movie Database (IMDb) offline files 
 downloaded from https://datasets.imdbws.com/
 
  If you use this program you must verify that you are compliant with the IMDb
 terms & conditions. This implementation bases on the specifications published
 on https://www.imdb.com/interfaces/ - accessed December 5th, 2019.
 
  This file is under Apache 2.0 license, see https://www.apache.org/licenses/

=cut

=head1 AUTHOR

  WinBrain
  --
  http://www.winbrain.org

  $Id: IMDb_tsv2csv $

=cut

use strict;
use warnings;
use Carp;
use utf8;
use Encode qw(decode);
use POSIX;
use File::Slurp;
use File::Copy;
use File::Basename;
use Readonly;
use Text::ParseWords;
use Data::Dumper qw(Dumper);
use DateTime;
use DateTime::Format::Strptime;
use Log::Log4perl;
use YAML::AppConfig;
use Win32::Locale;
use CLDR::Number;
use Getopt::Long 
    qw(GetOptionsFromArray :config ignore_case_always auto_version auto_help);
use Pod::Usage;
use List::Util qw[min max];
use Text::CSV;

our $VERSION = 0.5;

# logging & debug defaults
Readonly my $LOG_DEF_FIL_NAME    => './config/logging.config';
Readonly my $LOG_DEF_LOGGER      => 'myMovis';
Readonly my $LOG_DEF_WATCH_SEC   => 60;
Readonly my $DBG_DEF_DMP_NBR_LINES   => 0;

# configuration file default
Readonly my $INI_DEF_FIL_NAME    => './config/imdb_config.yaml';

# global defaults
Readonly my $STA_DEF_FIL_NAME    => './data/statistics.txt';
Readonly my $APP_DEF_LOCALE      => 'de_ch';
Readonly my $APP_DEF_TIM_FORMAT  => '%Y-%m-%d %H:%M:%S';
Readonly my $APP_DEF_DAT_DIRNAME => './data';

# datatype limits
Readonly my $SQL_TINYINT_MIN        => -128;
Readonly my $SQL_TINYINT_MAX        => 127;
Readonly my $SQL_TINYINT_UNSIGNED   => 255;
Readonly my $SQL_SMALLINT_MIN       => -32_768;
Readonly my $SQL_SMALLINT_MAX       => 32_767;
Readonly my $SQL_SMALLINT_UNSIGNED  => 65_535;
Readonly my $SQL_MEDIUMINT_MIN      => -8_388_608;
Readonly my $SQL_MEDIUMINT_MAX      => 8_388_607;
Readonly my $SQL_MEDIUMINT_UNSIGNED => 16_777_215;
Readonly my $SQL_INTEGER_MIN        => -2_147_483_648;
Readonly my $SQL_INTEGER_MAX        => 2_147_483_647;
Readonly my $SQL_INTEGER_UNSIGNED   => 4_294_967_295;
Readonly my $SQL_CHAR_MAXLEN     => 255;
Readonly my $SQL_VARCHAR_MAXLEN  => 65_532;

# global command line parameters, defaults set later
my (
    $out_par_opt_adult,     $ini_par_fil_name,        $app_par_int_locale,
    $out_par_opt_csv,       $out_par_opt_sql,            $out_par_opt_quiet,
    $app_glo_opt_verbose,   @inp_glo_fil_tsv,     @input_table_name,
    @input_comment, @input_foreign_keys, @input_primary_key,
    $app_glo_dir_data,      $log_glo_fil_config,    $log_use_logger,
    $log_level,     $log_par_watch_sec,          $log_glo_opt_debug,
    $log_glo_opt_info,      $dbg_dmp_nbr_lines,         $opt_manual,
    $option_file,   $sta_par_opt_nostat,   $sta_par_fil_name,
);

# global variables with information from yaml configuration file
my $ini_glo_datadir_name;    # directory to look for/write the data (files)
my $ini_glo_opt_adult;       # option if adult movie data should be included 
# global variables with field data from yaml configuration file
my @ini_glo_field_list;      # List with field names from yaml file
my %ini_glo_field_comment;   # Hash per field name with comments
my %ini_glo_field_datatype;  # Hash per field name with datatype definitions
my %ini_glo_field_format;    # Hash per field name with field formats
# global variables with file information from yaml configuration file
my @file_list;
my %file_dirname;
my %file_comment;
my %file_primary_key;
my %file_foreign_keys;
my %file_tablename;

# global variables settings, locale, locale formatter, command line backup
my $locale = $APP_DEF_LOCALE;   # default, migth be overwritten e.g. cmdline
my $unicode_locale_repository;
my $number_formatter;
my $param_config    = $INI_DEF_FIL_NAME;
my $param_logconfig = $LOG_DEF_FIL_NAME;
my $param_datadir   = $APP_DEF_DAT_DIRNAME;
my $param_loglevel  = 0;    # default from Log configuration file
my $param_dumplines = 0;    # default from Log configuration file
my $num_of_lines    = 0;
my @parameters      = @ARGV;
my $sta_glo_fil_handle;

# statistical global variables, arrays with information on data in columns
my (
  @sta_glo_maxlen,       # maximal column width (digits/characters) found
  @sta_glo_minlen,       # minimal column width (digits/characters) found

  @sta_glo_has_null,     # if column contains null values
  @sta_glo_null_count,   # number of null values in column
  
  @sta_glo_is_num,       # column only contains integers
  @sta_glo_is_signnum,   # column is signed, some integers are negative
  @sta_glo_max_num,      # max numeric value found in column
  @sta_glo_min_num,      # min numeric value found in column
  @sta_glo_num_count,    # number of numeric values in this column

  @sta_glo_is_dec,       # column only contains decimals
  @sta_glo_dec_digits,   # max number of digits in column
  @sta_glo_dec_decimals, # max number of digits after decimal point
  @sta_glo_dec_count     # number of decimal values in this column
);


=head2 format_number

  Format the number passed depending on the current locale.

=cut
sub format_number {
    my $number_to_format = shift;

    my $result_string = $number_formatter->format($number_to_format);
    return $result_string;
}

=head2 format_timestamp

  Format the date-time passed according to the pattern $APP_DEF_TIM_FORMAT.
  
=cut
sub format_timestamp {
    my $timestamp = shift;

    my $format =
      DateTime::Format::Strptime->new( pattern => $APP_DEF_TIM_FORMAT );
    my $timestamp_string = $format->format_datetime($timestamp);

    return $timestamp_string;
}

=head2 get_timedifference_in_minutes

  Calculate the number of minutes time difference between the two times given.
  
=cut
sub get_timedifference_in_minutes {
    my $start_time = shift;
    my $end_time   = shift;

	$start_time->set_time_zone('local');
	$end_time->set_time_zone('local');
    return $start_time->delta_ms($end_time)->in_units('minutes');
}

=head2 report_problem

  Exit program based on status_code passed. The hundreds define severity.
  
=cut
sub report_problem {
    my $error_number = shift;
    my $error_string = shift;
    
    # error level is derived from hundreds of error_number
    my Readonly $ERROR_LEVEL_GROUPING = 100;

    my $error_level = $error_number;
    if ( $error_number > $ERROR_LEVEL_GROUPING ) {
        $error_level = floor( $error_number / $ERROR_LEVEL_GROUPING );
    }
    if ( $error_string =~ /\n$/sxm ) {
        print {*STDERR} $error_string;
    }
    else {
        print {*STDERR} $error_string . "\n";
    }

    #    print {*STDERR} "***ERROR_$error_number: Processing aborted!\n";
    #    exit $error_level;
    confess("***ERROR_$error_number: Processing aborted!\n");
}

=head2 read_application_parameters

  Reading command line data.
  
=cut
sub read_application_parameters {
    my $parameter_ref = shift;

    # initialize values (global variables)
    #  - data configuration options
    $out_par_opt_adult               = 0;
    $ini_par_fil_name             = $INI_DEF_FIL_NAME;
    $app_par_int_locale = '';

    #  - output selection
    $out_par_opt_csv            = 1;
    $out_par_opt_sql            = 1;
    $sta_par_opt_nostat   = 0;
    $out_par_opt_quiet          = 0;
    $app_glo_opt_verbose        = 0;
    $sta_par_fil_name = $STA_DEF_FIL_NAME;

    #  - input configuration
    @inp_glo_fil_tsv     = ();
    @input_table_name   = ();
    @input_comment      = ();
    @input_foreign_keys = ();
    @input_primary_key  = ();
    $app_glo_dir_data           = '';

    #  - logging options
    $log_glo_fil_config = $LOG_DEF_FIL_NAME;
    $log_use_logger  = $LOG_DEF_LOGGER;
    $log_level       = '';
    $log_par_watch_sec       = $LOG_DEF_WATCH_SEC;
    $log_glo_opt_debug       = 0;
    $log_glo_opt_info        = 0;
    $dbg_dmp_nbr_lines      = 0;

    #  - help information
    $opt_manual = 0;

    #  - use file to read parameters
    $option_file = '';

    # process options
    my @parameters_in_use = @$parameter_ref;
    my $return_status     = GetOptionsFromArray(
        $parameter_ref,

        # data configuration
        'adult!'           => \$out_par_opt_adult,
        'config|yaml=s'    => \$ini_par_fil_name,
        'locale=s'         => \$app_par_int_locale,

        # output selection
        'csv!'             => \$out_par_opt_csv,
        'sql!'             => \$out_par_opt_sql,
        'nostatistics'     => \$sta_par_opt_nostat,
        'statisticsfile=s' => \$sta_par_fil_name, 
        'quiet'            => \$out_par_opt_quiet,
        'verbose'          => \$app_glo_opt_verbose,

        # input configuration
        'filename=s'       => \@inp_glo_fil_tsv,
        'tablename=s'      => \@input_table_name,
        'comment=s'        => \@input_comment,
        'foreignkey=s'     => \@input_foreign_keys,
        'primarykey=s'     => \@input_primary_key,
        'datadirectory=s'  => \$app_glo_dir_data,

        # logging options
        'logconfig=s'      => \$log_glo_fil_config,
        'logger=s'         => \$log_use_logger,
        'loglevel=s'       => \$log_level,
        'logwatch|watch=i' => \$log_par_watch_sec,
        'debug'            => \$log_glo_opt_debug,
        'info'             => \$log_glo_opt_info,
        'dumplines=i'      => \$dbg_dmp_nbr_lines,

        # help information
        'manual' => \$opt_manual,

        # use file to read parameters
        'optionfile=s' => \$option_file,
    );
    if ( !$return_status ) {
        print "You specified these command line perameters:\n";
        print join ' ', @parameters;
        print "\n";
        print "Use '" . basename($0) . " --help' for more information\n";
        report_problem( 903,
            "There was an error in the parameter(s) specified!\n" );
    }
#TODO xox if unprocessed content remains in array (not understood as parameters) croak
   if (@$parameter_ref) {
      # parameter list not empty means unprocessed content remained
      croak "You submitted parameters '@parameters_in_use'.\nThe following parameters are not understood '@$parameter_ref', use --help for usage."; 
   }
    
    if ($opt_manual) {
       #TODO define sections making up the full content
       pod2usage( -verbose => 99, 
                  -sections => "NAME|DESCRIPTION|OPTIONS|EXAMPLES|AUTHOR" );
    }
    
    analyze_optionfile_parameter();
}

=head2 analyze_optionfile_parameter

Reading parameters from a file in stead of the command line.
  
=cut
sub analyze_optionfile_parameter {
    if ($option_file) {
        if ( !-f $option_file ) {

            # file does not exist/is not accessible
            report_problem( 901, "Specified optionfile '$option_file' "
                               . "not found!" );
        }

        #TODO how to check if other options are specified
        if (0) {
            print "Command line perameters in use:\n";
            print join ' ', @parameters;
            print "\n";
            print "Use '" . basename($0) . " --help' for more information\n";
            report_problem( 901,
                            "Parameter 'optionfile' cannot be combined with "
                          . "other parameters!\n"
            );
        }

        # re-read parameters from file
        my @options = read_file( $option_file, chomp => 1 );

        # new backup copy of parameters, are changed by options file
        my $parameter_list = join ' ', @options;
        $parameter_list =~ s/\r/ /gsxm;
        $parameter_list =~ s/\n/ /gsxm;
        $parameter_list =~ s/\ \ /\ /gsxm;
        @parameters = shellwords($parameter_list);

        read_application_parameters( \@parameters );
        analyze_optionfile_parameter();
    }
}

=head2 analyze_remaining_parameters

Analyze rest of the command line parameters.
  
=cut
sub analyze_remaining_parameters {

    my $log_loc_logger = Log::Log4perl->get_logger($log_use_logger);
    $log_loc_logger->debug('analyzing remaining command line parameters');
    $log_loc_logger->info ('processing commandline ' . join " ", @parameters);

#print Dumper $option_ref;
    #TODO Logging of processing

    # if input files have been specified they overwrite config file entries
    my $number_of_files = @inp_glo_fil_tsv;

    if ($number_of_files) {
       $log_loc_logger->debug("$number_of_files files were specified via "
                    . "commandline/optionfile");
       my @local_file_list;
       my %local_file_dirname;
       my %local_file_tablename;
       my %local_file_comment;
       my %local_file_primary_key;
       my %local_file_foreign_keys;
       for (0..($number_of_files-1)) {
          my $counter = $_;         
          # process file entry, start by logging it
          $log_loc_logger->debug("file #$counter: '$inp_glo_fil_tsv[$counter]' "
                       . "specified via commandline/optionfile");

          #  - filename, store without path
          (my $local_file, my $local_directory, my $local_extension) = 
             fileparse($inp_glo_fil_tsv[$counter], '\.[^.]*$');
          $log_loc_logger->debug("filename splitt: directory '$local_directory', "
             . "file: '$local_file', extension:'$local_extension'");
          my $local_file_list_entry;
          if (lc $local_extension eq '.gz') {
             $local_file_list_entry = $local_file;
          } else {
             $local_file_list_entry = $local_file . $local_extension;
          }
          push @local_file_list, $local_file_list_entry;
          $local_file_dirname{$local_file_list_entry} = $local_directory;
          $log_loc_logger->debug("list-entry of files to process: directory "
             . "'$local_directory', file: '$local_file_list_entry'");

          #  - tablename
          my $local_file_tablename_entry;
          if (defined($input_table_name[$counter])) {
             $local_file_tablename_entry = $input_table_name[$counter];
          } elsif (defined($file_tablename{$local_file_list_entry})) {
             $local_file_tablename_entry = 
                                      $file_tablename{$local_file_list_entry};
          } else {
             # no information on tablename found, base name on filename
             $local_file_tablename_entry = $local_file_list_entry;
             $local_file_tablename_entry =~ s/\./_/gsxm;
          }
          $local_file_tablename{$local_file_list_entry} = 
                                                  $local_file_tablename_entry;
          $log_loc_logger->debug("file #$counter: '$inp_glo_fil_tsv[$counter]' has "
                       . "tablename '$local_file_tablename_entry' assigned");

          #  - comment
          my $local_file_comment_entry;
          if (defined($input_comment[$counter])) {
             $local_file_comment_entry = $input_comment[$counter];
          } elsif (defined($file_comment{$local_file_list_entry})) {
             $local_file_comment_entry = $file_comment{$local_file_list_entry};
          } else {
             # no information on comment found, base comment on filename
             $local_file_comment_entry = "Table based on IMDB import from file"
                                       . " $local_file_list_entry";
          }
          $local_file_comment{$local_file_list_entry} = 
                                                  $local_file_comment_entry;
          $log_loc_logger->debug("file #$counter: '$inp_glo_fil_tsv[$counter]' has "
                       . "comment '$local_file_comment_entry' assigned");

          #  - primary_key
          my $local_file_primary_key_entry;
          if (defined($input_primary_key[$counter])) {
             $local_file_primary_key_entry = $input_primary_key[$counter];
          } elsif (defined($file_primary_key{$local_file_list_entry})) {
             $local_file_primary_key_entry = 
                                    $file_primary_key{$local_file_list_entry};
          } else {
             # no information on primary key found, set to empty
             $local_file_primary_key_entry = '';
          }
          $local_file_primary_key{$local_file_list_entry} = 
                                                $local_file_primary_key_entry;
          $log_loc_logger->debug("file #$counter: '$inp_glo_fil_tsv[$counter]' has "
                    . "primary key '$local_file_primary_key_entry' assigned");

          #  - foreign_key
          my $local_file_foreign_keys_entry;
          if (defined($input_foreign_keys[$counter])) {
             $local_file_foreign_keys_entry = $input_foreign_keys[$counter];
          } elsif (defined($file_foreign_keys{$local_file_list_entry})) {
             $local_file_foreign_keys_entry = 
                                   $file_foreign_keys{$local_file_list_entry};
          } else {
             # no information on foreign keys found, set to empty
             $local_file_foreign_keys_entry = '';
          }
          $local_file_foreign_keys{$local_file_list_entry} = 
                                   $local_file_foreign_keys_entry;
          $log_loc_logger->debug("file #$counter: '$inp_glo_fil_tsv[$counter]' has "
                   . "foreign keys '$local_file_foreign_keys_entry' assigned");
       }
       # all values defined, overwrite existing lists with new ones
       $log_loc_logger->debug("overwriting file/table information from config file "
                    . "with commandline information");
       @file_list = @local_file_list;
       %file_dirname = %local_file_dirname;
       %file_tablename = %local_file_tablename;
       %file_comment = %local_file_comment;
       %file_primary_key = %local_file_primary_key;
       %file_foreign_keys = %local_file_foreign_keys;
    }

    if ($app_glo_dir_data) {
       # directory name must end with a slash, append one if needed
       if (!($app_glo_dir_data =~ /\/$/)) {
          $app_glo_dir_data .= '/';
       }
       $ini_glo_datadir_name = $app_glo_dir_data;
       $log_loc_logger->debug("overwriting data directory from default/config with "
                    . "'$app_glo_dir_data' from commandline");
    }

    # get locale to be used
    if ($app_par_int_locale) {
       $locale = $app_par_int_locale;
       $log_loc_logger->debug("overwriting locale from default/config with "
                    . "'$app_par_int_locale' from commandline");
    } else {
       if ( Win32::Locale::get_locale() ) {
          $locale = Win32::Locale::get_locale();
          $log_loc_logger->debug("overwriting locale from default/config with "
                       . "'$locale' from operating system");
       }
    $unicode_locale_repository = CLDR::Number->new( locale => $locale );
    $number_formatter = $unicode_locale_repository->decimal_formatter;
    }
    $log_loc_logger->debug('done with analyzing remaining command line parameters');
}

=head2 initialize_logging

Inizialise logging based on parameter file.
  
=cut
sub initialize_logging {

    my $log_glo_fil_config_to_use = $LOG_DEF_FIL_NAME;
    if ($log_glo_fil_config) {
       $log_glo_fil_config_to_use = $log_glo_fil_config;
    }

    my $log_par_watch_sec_seconds_to_use = $LOG_DEF_WATCH_SEC;
    if ($log_par_watch_sec) {
       $log_par_watch_sec_seconds_to_use = $log_par_watch_sec;
    }

    my $log_level_to_use;
    if (($log_level) && ($log_level ne 'default')) {
       $log_level_to_use = $log_level;
    }
    if ($log_glo_opt_info) {
       $log_level_to_use = 'INFO';
    }
    if ($log_glo_opt_debug) {
       $log_level_to_use = 'DEBUG';
    }
       
    $log_glo_fil_config = $log_glo_fil_config_to_use;
    $log_level       = $log_level_to_use;
    $log_par_watch_sec       = $log_par_watch_sec_seconds_to_use;
    
    # configuration loaded, start logging
    if ( !-f $log_glo_fil_config ) {
        confess("Logging configuration file '$log_glo_fil_config' not"
              . " found, processing aborted!\n" );
    }
    
    Log::Log4perl->init_and_watch( $log_glo_fil_config_to_use, 
                                   $log_par_watch_sec_seconds_to_use );

#TODO muss das nur einmal gesetzt werden, oder immer wieder beim Logger holen?
    my $log_loc_logger = Log::Log4perl->get_logger($log_use_logger);
    if ($log_level_to_use) {
       $log_loc_logger->level($log_level_to_use);
    }
    
    my $log_message = "logging started: config '$log_glo_fil_config_to_use', "
                    . "logger '$log_use_logger', read config every "
                    . "$log_par_watch_sec_seconds_to_use seconds";
    if ($log_level) {
       $log_message = $log_message . ", log level '$log_level'"; 
    }
    $log_loc_logger->debug( $log_message );

    return $log_loc_logger;
}

=head2 read_configuration_from_yaml_file

Readig data specifications from yaml file.
  
=cut
sub read_configuration_from_yaml_file {

    #TODO include error handling for file not exist or any key not exist
    #TODO confess with error message if any problems occur
    # get logger to enable logging, start logging
    my $log_loc_logger = Log::Log4perl->get_logger($log_use_logger);
    $log_loc_logger->debug('starting read_configuration_from_yaml_file()');

    # read configuration file to object yaml_config
    my $yaml_config = YAML::AppConfig->new( file => $INI_DEF_FIL_NAME );
    $ini_glo_datadir_name = $yaml_config->get_IMDB()->{'directory'};

    # get exclude adult configuration, adapt perl boolean (0 false, rest true)
    $ini_glo_opt_adult = $yaml_config->get_IMDB()->{'exclude_adult'};
    if ( ( lc $ini_glo_opt_adult eq 'false' ) or ( lc $ini_glo_opt_adult eq 'no' ) ) {
        $ini_glo_opt_adult = 0;
    }

    # load all file/table definitions from configuration file
    #TODO FOR-LOOP
    for (
       my $counter = 0 ;
       defined( my $files = $yaml_config->get_IMDB()->{'files'}[$counter] ) ;
       $counter++
       )
    {  
       my $file_name = 
          $yaml_config->get_IMDB()->{'files'}[$counter]{'filename'};
       # if filename originates from download spec remove its .gz extension
       $file_name =~ s/\.gz$//gsxm;
       (my $local_file, my $local_directory, my $local_extension) = 
             fileparse($file_name, '\.[^.]*$');
       $log_loc_logger->debug("filename splitt: directory '$local_directory', file:"
             . " '$local_file', extension:'$local_extension'");
    	$file_name = $local_file . $local_extension;
        push @file_list, $file_name;
        # use $ini_glo_datadir_name if directory was not really specified in filename
        if (($ini_glo_datadir_name) and ($local_directory eq './')) {
        	if (!($file_name =~ /\.\//sxm)) {
        		$local_directory = $ini_glo_datadir_name;
        	}
        }
        $file_dirname{ $file_name } =  $local_directory;
        $file_comment{ $file_name } =
          $yaml_config->get_IMDB()->{'files'}[$counter]{'comment'};
        $file_primary_key{ $file_name } =
          $yaml_config->get_IMDB()->{'files'}[$counter]{'primary_key'};
        $file_foreign_keys{ $file_name } =
          $yaml_config->get_IMDB()->{'files'}[$counter]{'foreign_keys'};
        $file_tablename{ $file_name } =
          $yaml_config->get_IMDB()->{'files'}[$counter]{'tablename'};
    }

    # load all column definitions from configuration file
    #TODO FOR-LOOP
    for (
        my $counter = 0 ;
        defined( my $files = $yaml_config->get_IMDB()->{'fields'}[$counter] );
        $counter++
      )
    {
        push @ini_glo_field_list,
          $yaml_config->get_IMDB()->{'fields'}[$counter]{'column'};
        $ini_glo_field_comment{ $yaml_config->get_IMDB()
              ->{'fields'}[$counter]{'column'} } =
          $yaml_config->get_IMDB()->{'fields'}[$counter]{'comment'};
        $ini_glo_field_datatype{ $yaml_config->get_IMDB()
              ->{'fields'}[$counter]{'column'} } =
          $yaml_config->get_IMDB()->{'fields'}[$counter]{'datatype'};
        $ini_glo_field_format{ $yaml_config->get_IMDB()
              ->{'fields'}[$counter]{'column'} } =
          $yaml_config->get_IMDB()->{'fields'}[$counter]{'format'};
    }
    $log_loc_logger->debug('done with read_configuration_from_yaml_file()');

    #TODO DUMP CONFIGURATION JUST READ to DEBUG logger
}

=head2 open_for_write

Return handle of a file opened for write with various options.

=cut
sub open_for_write {
    my ( $param_filename, $param_shortdesc, %param_options ) = @_;
    my $param_defaultfile = delete $param_options{'defaultfile'} // undef;
    my $param_createdir = delete $param_options{'createdir'} // 0;
    my $param_rename = delete $param_options{'rename'} // 1;
    my $param_errornum = delete $param_options{'errornum'} // -1;
    my $param_logger = delete $param_options{'logger'} // 
                       $LOG_DEF_LOGGER;
    if (keys %param_options) {
       croak "Optional parameters are 'defaultfile','createdir','rename',"
           . "'errornum','logger'.\nUnknown/unexpected parameters: '", 
           join ',', keys %param_options, "'";
    }

    Readonly my $INFO_LEVELS  =>  99;
    Readonly my $WARN_LEVELS  => 399;
    Readonly my $ERROR_LEVELS => 799;
    Readonly my $FATAL_LEVELS => 999;
    
    my $out_loc_fil_handle;
    my $local_continue_on_error = (($param_errornum > 0) && 
                                   ($param_errornum <= $WARN_LEVELS));

    # get logger to report to logfile(s)
    my $log_loc_logger = Log::Log4perl->get_logger($param_logger);
    # cannot log variable $param_defaultfile if undef, so report string <undef>
    my $local_defaultfile = $param_defaultfile;
    if (!$local_defaultfile) {
    	$local_defaultfile = '<undef>';
    }
    $log_loc_logger->debug("sub open_for_write params: file '$param_filename', " 
             . "descript '$param_shortdesc' [defaultfile '$local_defaultfile',"
             . "createdir '$param_createdir', rename'$param_rename', "
             . "errornum' $param_errornum', logger'$param_logger']");
    
    # standardize directory names, make sure we have only forward slashes
    my $local_cleanname = $param_filename;
    $local_cleanname =~ s/\\/\//gsxm;
    if ($local_cleanname ne $param_filename) {
       $log_loc_logger->debug("directory name cleanup - changed filename from "
                    . "'$param_filename' to '$local_cleanname'");        
    }

    # check if only directoryname specified, without filename
    if (-d $local_cleanname) {
       if (defined $param_defaultfile ) {
          $log_loc_logger->debug("default filename '$param_defaultfile' added to "
                       . "directory '$local_cleanname': "
                       . "'$local_cleanname$param_defaultfile'");
          $local_cleanname .= $param_defaultfile;
       } else {
       	  if ($local_continue_on_error) {
             report_problem ($param_errornum, "Directory '$param_filename' "
                           . "specified for $param_shortdesc, "
                           . "but filename is missing.");
             return undef;
       	  } else {
             confess ("Directory '$param_filename' specified for "
                    . "$param_shortdesc, filename missing.");
       	  }
       }
    }

    # splitt name provided into its parts
    (my $local_file, my $local_directory, my $local_extension) = 
             fileparse($param_filename, '\.[^.]*$');
    $log_loc_logger->debug("filename splitt: directory '$local_directory', file:"
             . " '$local_file', extension:'$local_extension'");
  
    # check if directory exists
    if (!-d $local_directory) {
       if ($param_createdir) {
          if (make_path ($local_directory)) {
             $log_loc_logger->info("successfully created directory $local_directory"
                         . " for $param_shortdesc.");
          } else {
       	     if ($local_continue_on_error) {
                $log_loc_logger->error("failed to create directory $local_directory "
                             . "for $param_shortdesc.");
                report_problem ($param_errornum, "Was not able to create "
                             . "directory $local_directory for "
                             . "$param_shortdesc.");
                return undef;
          	 } else {
                croak "Failed to create directory $local_directory for "
                    . "$param_shortdesc..";
       	     }
          }
       } else {
          $log_loc_logger->error("specified $param_shortdesc directory "
                       . "'$local_directory' does not exist.");
       	  if ($local_continue_on_error) {
             $log_loc_logger->error("continuing in spite of error as sub "
                           . "open_for_write is set to continue on error.");
             report_problem ($param_errornum, "Specified $param_shortdesc "
                           . "directory '$local_directory' does not exist.");
             return undef;
       	  } else {
             croak "Specified directory '$local_directory' does not exist.";
       	  }
       }
    }
    
    # rename file if one with same name already exists (add timestamp & .save)
    if ((-f $param_filename) && ($param_rename)) {
       $param_filename =~ /(.*)\.([^\.]*)$/isxm;
       my $local_timestamp = strftime "%Y%m%d%H%M%S", localtime time;
       my $local_backupname = $1 . $local_timestamp . '_' . $2 . '.save';
       if (!(move($param_filename, $local_backupname))) {
          $log_loc_logger->error("specified $param_shortdesc file '$param_filename' "
                       . "exists and not able to rename it to "
                       . "'$local_backupname' $!.");
          report_problem ($param_errornum, "$param_shortdesc '$param_filename'"
                        . " already exists and not able to rename it to "
                        . "'$local_backupname' $!.");
       } 
    }
    
    # open file
    if (!(open( $out_loc_fil_handle, '>:encoding(UTF-8)', $local_cleanname ))) {
       $log_loc_logger->error("Cannot open $param_shortdesc '$param_filename' $!");
       if ($local_continue_on_error) {
          $log_loc_logger->error("continuing in spite of error as sub open_for_write"
                       . " is set to continue on error.");
          report_problem ($param_errornum, 
                        "Cannot open $param_shortdesc '$param_filename' $!.");
          return undef;
       } else {
          confess ("Cannot open $param_shortdesc '$param_filename' $!.");
       }
    }

    if (!defined($out_loc_fil_handle)) {
       $log_loc_logger->warn("Sub open_to_write ends unable to return the "
                   . "$param_shortdesc filehandle for '$param_filename' $!.");
    }
    return $out_loc_fil_handle; 
}

=head2 get_statistical_information

Put together a string with table and column statistics.
  
=cut
sub get_statistical_information {
    my $num_columns      = shift;
    my $column_names_ref = shift;

    my @column_names = @$column_names_ref;
    my $statistical_string;

    $statistical_string =
        format_number($num_of_lines)
      . " lines, "
      . format_number($num_columns)
      . " columns:\n";
    my $max_columnwidth = 0;
    for(0..($num_columns-1)) {
       my $counter = $_; 
       $max_columnwidth = max($max_columnwidth, length $column_names[$counter]);
    }
    for(0..($num_columns-1)) {
       my $counter = $_;
       $statistical_string .=
          '   ' . sprintf("%03d", $counter+1) . ': ' . $column_names[$counter]
                . (' ' x ($max_columnwidth - length $column_names[$counter]));
       if ( $sta_glo_minlen[$counter] > $sta_glo_maxlen[$counter] ) {
          $statistical_string .= ' no data - only NULLs';
       }
       else {
          $statistical_string .=
               ' length min/max '
             . format_number( $sta_glo_minlen[$counter] ) . '/'
             . format_number( $sta_glo_maxlen[$counter] );
       }
       if ( $sta_glo_is_num[$counter] ) {
          # it is a string
          if ( $sta_glo_is_signnum[$counter] ) {
               $statistical_string .= ', SIGNED ';
          }
          else {
               $statistical_string .= ', UNSIGNED ';
          }
          if ( $sta_glo_is_dec[$counter] ) {
               $statistical_string .=
                   'DECIMAL ['
                 . $sta_glo_dec_digits[$counter] . "."
                 . $sta_glo_dec_decimals[$counter] . '] (range ';
          }
          else {
               $statistical_string .= 'NUMERIC (range ';
          }
          $statistical_string .=
              format_number( $sta_glo_min_num[$counter] ) . '..'
            . format_number( $sta_glo_max_num[$counter] ) . ")";
       } else {
          # it is a string
          if ($sta_glo_maxlen[$counter] <= $SQL_CHAR_MAXLEN) {
             $statistical_string .= ', CHAR';
          } elsif ($sta_glo_maxlen[$counter] <= $SQL_VARCHAR_MAXLEN) {
             $statistical_string .= ', VARCHAR';
          } else {
             $statistical_string .= ', TEXT';
          }
       }
       if ( $sta_glo_has_null[$counter] ) {
            $statistical_string .=
               ', NULLs ('
             . format_number( $sta_glo_null_count[$counter] )
             . ' times)';
       } else {
           $statistical_string .=
               ', NO nulls';
       }
       $statistical_string .= "\n";
    }
    return $statistical_string;
}

=head2 get_datatype

Returns the string with the SQL datatype of a field based on its statistics.
  
=cut
sub get_datatype {
    my $counter = shift;

    my $data_type_string;
    if ( $sta_glo_is_num[$counter] ) {

        # numeric
        if ( $sta_glo_is_dec[$counter] ) {

            # DECIMAL
            $data_type_string =
                'DECIMAL ('
              . $sta_glo_dec_digits[$counter] . ","
              . $sta_glo_dec_decimals[$counter] . ")";
            if ( $sta_glo_is_signnum[$counter] ) {
                $data_type_string .= ' SIGNED';
            }
            else {
                $data_type_string .= ' UNSIGNED';
            }
        }
        else {
            # INTEGER
            if ( $sta_glo_is_signnum[$counter] ) {

                # signed numbers
                if (   ( $sta_glo_min_num[$counter] >= $SQL_TINYINT_MIN )
                    && ( $sta_glo_max_num[$counter] <= $SQL_TINYINT_MAX ) )
                {
                    $data_type_string = 'TINYINT';
                }
                elsif (( $sta_glo_min_num[$counter] >= $SQL_SMALLINT_MIN )
                    && ( $sta_glo_max_num[$counter] <= $SQL_SMALLINT_MAX ) )
                {
                    $data_type_string = 'SMALLINT';
                }
                elsif (( $sta_glo_min_num[$counter] >= $SQL_MEDIUMINT_MIN )
                    && ( $sta_glo_max_num[$counter] <= $SQL_MEDIUMINT_MAX ) )
                {
                    $data_type_string = 'MEDIUMINT';
                }
                elsif (( $sta_glo_min_num[$counter] >= $SQL_INTEGER_MIN )
                    && ( $sta_glo_max_num[$counter] <= $SQL_INTEGER_MAX ) )
                {
                    $data_type_string = 'INTEGER';
                }
                else {
                    $data_type_string = 'BIGINT';
                }
                $data_type_string .= ' SIGNED';
            }
            else {
                # unsigned numbers
                if ( $sta_glo_max_num[$counter] <= $SQL_TINYINT_UNSIGNED ) {
                    $data_type_string = 'TINYINT';
                }
                elsif ( $sta_glo_max_num[$counter] <= $SQL_SMALLINT_UNSIGNED ) {
                    $data_type_string = 'SMALLINT';
                }
                elsif ( $sta_glo_max_num[$counter] <= $SQL_MEDIUMINT_UNSIGNED ) {
                    $data_type_string = 'MEDIUMINT';
                }
                elsif ( $sta_glo_max_num[$counter] <= $SQL_INTEGER_UNSIGNED ) {
                    $data_type_string = 'INTEGER';
                }
                else {
                    $data_type_string = 'BIGINT';
                }
                $data_type_string .= ' UNSIGNED';
            }
        }
    }
    else {
        # string
        if ( $sta_glo_maxlen[$counter] <= $SQL_CHAR_MAXLEN ) {
            $data_type_string = 'CHAR (' . $sta_glo_maxlen[$counter] . ")";
        }
        elsif ( $sta_glo_maxlen[$counter] <= $SQL_VARCHAR_MAXLEN ) {
            $data_type_string = 'VARCHAR (' . $sta_glo_maxlen[$counter] . ")";
        }
        else {
            $data_type_string = 'TEXT (' . $sta_glo_maxlen[$counter] . ")";
        }
    }
    if ( $sta_glo_has_null[$counter] ) {
        $data_type_string .= ' NULL,';
    }
    else {
        $data_type_string .= ' NOT NULL,';
    }
    return $data_type_string;
}

=head2 write_sql_create_table

Writes a .sql file with CREATE TABLE for the data found in the .csv file.
  
=cut
sub write_sql_create_table {
    my $sql_filename    = shift;
    my $lookup_filename = shift;
    my $num_of_columns  = shift;
    my $column_name_ref = shift;

    my @column_names       = @$column_name_ref;
    my $table_name         = $file_tablename{$lookup_filename};
    my $table_comment      = $file_comment{$lookup_filename};
    my $table_primary_key  = $file_primary_key{$lookup_filename};
    my $table_foreign_keys = $file_foreign_keys{$lookup_filename};

    my $sql_out_handle = open_for_write ($sql_filename, 'SQL file', 
                            createdir => 1, rename => 1, errornum => 998);

    print {$sql_out_handle} "DROP TABLE IF EXISTS $table_name;\n";
    print {$sql_out_handle} "CREATE TABLE $table_name (\n";
    for(0..($num_of_columns-1)) {
    	my $counter = $_; 
        print {$sql_out_handle} '   ' . lc $column_names[$counter] . ' ';
        print {$sql_out_handle} get_datatype($counter) . "\n";
    }
    if ($table_primary_key) {
        print {$sql_out_handle} '   CONSTRAINT pk_'
          . $table_name
          . " PRIMARY KEY ($table_primary_key)\n";
    }
    if ($table_foreign_keys) {
#TODO xoxo implement loop
#       foreach ($table_foreign_keys =~ /^\s*\(([^\)]+)\)\s*([^,]+)(,\s*\(([^\)]+)\)*)\s*([^,]+)$/sxm) {
#          #TODO process $1 $3
       my $table_foreign_key_column = 'musterspalte';
       my $table_foreign_key_table = 'meinetabelle';
       print {$sql_out_handle} '   CONSTRAINT fk_' . $table_foreign_key_table
                             . " FOREIGN KEY ($table_foreign_key_column) "
                             . " REFERENCES $table_foreign_key_table ($table_foreign_key_column)\n";
#       }
    }
    print {$sql_out_handle} "   )\n";
    print {$sql_out_handle} "   COMMENT = '$table_comment';\n";

#TODO Create indices: CREATE OR REPLACE [UNIQUE|FULLTEXT|SPATIAL] INDEX [IF NOT EXISTS] index_name ON $table_name (index_col_name,...)

    print {$sql_out_handle} "\n/*\n\n";
#TODO use lookup filename to look up filename with path
    print {$sql_out_handle} "This sql bases on the information gathered from\n"
      . "   file '$lookup_filename' - ";
    print {$sql_out_handle} get_statistical_information( $num_of_columns,
        \@column_names );
    print {$sql_out_handle} "\n   File created with script " . basename($0) . ' at ' . format_timestamp(DateTime->now()) . "\n\n";
    print {$sql_out_handle} "*/\n";
    close $sql_out_handle;
}

=head2 process_file

Process given .tsv file according to options from configuration & commandline.

=cut
sub process_file {
    my $lookup_filename  = shift;
    my $tsv_in_filename  = shift;
    my $csv_out_filename = shift;
    my $sql_out_filename = shift;

    my $has_error = 0;

    #TODO add counters for skipped adult content

    #TODO verify that file exists
    open( my $tsv_in_handle, '<:encoding(UTF-8)', $tsv_in_filename )
      or confess("Can't open $tsv_in_filename: $!");
    my $tsv_in = Text::CSV->new(
        {
            binary         => 1,
            eol            => "\n",
            sep_char       => "\t",
            undef_str      => '\N',
            blank_is_undef => 1,
            empty_is_undef => 1,
            escape_char    => undef,
            quote_char     => undef
        }
    );
    $tsv_in->column_names( $tsv_in->getline($tsv_in_handle) );    # use header
    my $num_of_columns = $tsv_in->column_names();
    ( my @column_names ) = $tsv_in->column_names();

    my $csv_out_handle;
    my $csv_out;
    if ($csv_out_filename) {

       # open file if one is requested
       #TODO rename if already exists with timestamp _save_timestamp
       my $csv_out_handle = open_for_write ($csv_out_filename, 'CSV file', 
                                createdir => 1, rename => 0, errornum => 997);
       $csv_out = Text::CSV->new( { binary => 1, eol => "$/" } );
       $csv_out->print( $csv_out_handle, \@column_names );
    }

    # show progress on console / write statistics
    if ($sta_glo_fil_handle && !$sta_par_opt_nostat) {
       print {$sta_glo_fil_handle} "$tsv_in_filename - ";
    }
    if (!$out_par_opt_quiet) {
       print "$tsv_in_filename - ";
    }

    # initialize variables for statistical information
    for(0..($num_of_columns-1)) {
    	my $counter = $_;
        @sta_glo_maxlen[$counter]            = 0;
        @sta_glo_minlen[$counter]            = 9999;
        @sta_glo_has_null[$counter]          = 0;
        @sta_glo_null_count[$counter]        = 0;
        @sta_glo_is_num[$counter]        = 1;
        @sta_glo_is_signnum[$counter] = 0;
        @sta_glo_max_num[$counter]        = 0;
        @sta_glo_min_num[$counter]        = 0;
        @sta_glo_is_dec[$counter]        = 0;
        @sta_glo_dec_digits[$counter]    = 0;
        @sta_glo_dec_decimals[$counter]  = 0;
        @sta_glo_num_count[$counter]     = 0;
        @sta_glo_dec_count[$counter]     = 0;
    }
    my $num_format_errors = 0;

    # process all lines in the file, $last_line stores last line processed
    my $lastLine;
    while ( my $line = $tsv_in->getline($tsv_in_handle) ) {
        $num_of_lines++;
        $lastLine = $line;
        if ($csv_out_handle && $csv_out_filename) {
            $csv_out->print( $csv_out_handle, \@$line );
        }
        if ( $num_of_lines <= $DBG_DEF_DMP_NBR_LINES ) {
            print $num_of_lines . ": ";
        }

        # process all columns of the line
        for(0..($num_of_columns-1)) {
            my $counter = $_;
            if (   ( !defined( @$line[$counter] ) )
                or ( @$line[$counter] eq '\N' )
                or ( @$line[$counter] eq '' ) )
            {
             # null entry -> remember sta_glo_has_null for column, no further processing
                if ( $num_of_lines <= $DBG_DEF_DMP_NBR_LINES ) {
                    print ' <NULL>';
                }
                $sta_glo_has_null[$counter] = 1;
                $sta_glo_null_count[$counter]++;
            }
            else {
                # check format
                my $format_required =
                  $ini_glo_field_format{ $column_names[$counter] };
                my $entry       = @$line[$counter];
                my $line_key    = @$line[0];
                my $column_name = $column_names[$counter];
                my $result      = $entry =~ /$format_required/isxm;
                if ( !$result ) {
                    $num_format_errors++;
                    print
"WARNING, number format errors! column $column_name in $line_key: value '$entry' "
                      . "is not matching format $format_required\n";
                    for(0..($num_of_columns-1)) { 
                        my $display_counter = $_;
                        print $column_names[$display_counter] . ': '
                          . @$line[$display_counter] . ' ('
                          . $ini_glo_field_format{ $column_names[$display_counter] }
                          . ")\n";
                    }
                }
                if ( $num_of_lines <= $DBG_DEF_DMP_NBR_LINES ) {
                    print " " . @$line[$counter];
                }
                if ( length( @$line[$counter] ) > $sta_glo_maxlen[$counter] ) {
                    $sta_glo_maxlen[$counter] = length( @$line[$counter] );
                }
                if ( length( @$line[$counter] ) < $sta_glo_minlen[$counter] ) {
                    $sta_glo_minlen[$counter] = length( @$line[$counter] );
                }
                if ( @$line[$counter] =~ /^\s*(\d+)\s*$/sxm ) {
                    $sta_glo_num_count[$counter]++;
                    if ( $1 > $sta_glo_max_num[$counter] ) {
                        $sta_glo_max_num[$counter] = $1;
                    }
                }
                elsif ( @$line[$counter] =~ /^\s*(-)?\s*(\d+)\s*$/sxm ) {
                    $sta_glo_num_count[$counter]++;
                    if ($1) {
                        if ( -1 * $2 < $sta_glo_min_num[$counter] ) {
                            $sta_glo_min_num[$counter] = -1 * $2;
                        }
                    }
                    else {
                        if ( $2 > $sta_glo_max_num[$counter] ) {
                            $sta_glo_max_num[$counter] = $2;
                        }
                    }
                }
                elsif ( @$line[$counter] =~ /^\s*(-)?\s*(\d+)\.(\d*)\s*$/sxm ) {
                    $sta_glo_is_dec[$counter] = 1;
                    $sta_glo_num_count[$counter]++;
                    my $sign = $1;
                    if ($1) {
                        $sta_glo_is_signnum[$counter] = 1;
                        $sta_glo_min_num[$counter] = min( $sta_glo_min_num[$counter],
                            floor( @$line[$counter] ) );
                    }
                    else {
                        $sta_glo_max_num[$counter] = max( $sta_glo_max_num[$counter],
                            ceil( @$line[$counter] ) );
                    }
                    my $length2 = 0;
                    my $length3 = 0;
                    if ($2) { $length2 = length($2) }
                    if ($3) { $length3 = length($3) }
                    $sta_glo_dec_digits[$counter] =
                      max( $sta_glo_dec_digits[$counter], $length2 + $length3 );
                    $sta_glo_dec_decimals[$counter] =
                      max( $sta_glo_dec_decimals[$counter], length($3) );
                }
                else {
                    $sta_glo_is_num[$counter] = 0;
                    $sta_glo_is_dec[$counter] = 0;
                }
            }
        }
        if ( $num_of_lines <= $DBG_DEF_DMP_NBR_LINES ) {
            print "\n";
        }
    }

    # print results
    if ($sta_glo_fil_handle && !$sta_par_opt_nostat) {
    	print {$sta_glo_fil_handle} get_statistical_information( $num_of_columns, \@column_names );
        print {$sta_glo_fil_handle} "\n";
    }
    if (!$out_par_opt_quiet) {
       print get_statistical_information( $num_of_columns, \@column_names );
    }
    if ( !eof $tsv_in_handle ) {
        $has_error = 1;
        print
"   ERROR !!! FILE CONTAINS MORE LINES - SOME LINES REMAIN UNPROCESSED !!!\n";
        print '   Last line processed: ';
        for(0..($num_of_columns-1)) {
        	my $counter = $_;
            if (   ( !defined( @$lastLine[$counter] ) )
                or ( @$lastLine[$counter] eq '\N' ) )
            {
                print ' <NULL>';
            }
            else {
                print " " . @$lastLine[$counter];
            }
        }
        print "\n";
    }
    if (!$out_par_opt_quiet) {
       print "\n";
    }

    # close TSV & CSV files
    close $tsv_in_handle;
    if ($csv_out_handle && $csv_out_filename) {
        close $csv_out_handle;
    }
    if ($sql_out_filename) {

        # if requested write sql to drop and create table
        write_sql_create_table(
            $sql_out_filename, $lookup_filename,
            $num_of_columns,   \@column_names
        );
    }
    return $has_error;
}

######################
#### MAIN PROGRAM ####
######################

# primary initialization, note start time, read command line parameters
binmode( STDOUT, ':utf8' );

# reading parameters (from commandline or from file if optionfile specified)
read_application_parameters( \@ARGV );
my $log_loc_logger = initialize_logging();
read_configuration_from_yaml_file();
analyze_remaining_parameters();

if (($ini_glo_datadir_name) && (!-d $ini_glo_datadir_name)) {
   $log_loc_logger->error("directory specified to look for file '$ini_glo_datadir_name' "
                . "does not exist");
   report_problem (906, "Directory specified to look for file "
                . "'$ini_glo_datadir_name' does not exist.");
}

my $app_start_time = DateTime->now();
# open statistics file if one was requested
if (!$sta_par_opt_nostat) {
   $sta_glo_fil_handle = open_for_write ($sta_par_fil_name, 
                                     'Statistics file', createdir => 1, 
                                      rename => 1, errornum => 909);
   if (!defined($sta_glo_fil_handle)) {
#TODO log a warning, send out a message
      $sta_par_opt_nostat = 1;
   } else {
     print {$sta_glo_fil_handle} 'Processing starts ' . 
                              format_timestamp($app_start_time) . " ...\n\n";
   }
}

if (!$out_par_opt_quiet) {
   print 'Processing starts ' . format_timestamp($app_start_time) . " ...\n\n";
}

# process all file entries received from config or commandline
foreach my $filename (@file_list) {

    # check if file exists, try to find it
    my $in_filename = $filename;
    if ($file_dirname{$filename}) {
       if (-f $file_dirname{$filename} . $in_filename) {
          $in_filename = $file_dirname{$filename} . $in_filename;    	
       } elsif (-f $ini_glo_datadir_name . $in_filename) {
          $in_filename = $ini_glo_datadir_name . $in_filename; 
          $file_dirname{$filename} = $ini_glo_datadir_name;    	          
       } elsif (-f $in_filename) {
          $file_dirname{$filename} = './';    	          
       }
    }

    if ( !-f $in_filename ) {
       $log_loc_logger->warn("file '$in_filename' not found, skipping processing of "
                   . "this file");
       report_problem (90, "File '$in_filename' not found, skipping processing"
                         . " of this file.");
       next;
    }

    # TODO archive any already existing .csv or .sql by renaming them
    # derive out_filename from in_filename, turn .tsv into .csv
    my $out_filename = $in_filename;
    $out_filename =~ s/.tsv$/.csv/isxm;

    # construct sql_filename, same directory as source file
    my $dirname  = dirname($in_filename);
    if ($dirname) {
       $dirname .= '/';
    }
    my $sql_filename =
        $dirname
      . 'drop_create_table_'
      . $file_tablename{$filename} . '.sql';

    process_file( $filename, $in_filename, $out_filename, $sql_filename );
}

my $app_end_time = DateTime->now();
my $runtime_minutes =
   get_timedifference_in_minutes( $app_start_time, $app_end_time );

if ($sta_glo_fil_handle && !$sta_par_opt_nostat) {
   print {$sta_glo_fil_handle} 'processing completed ' . format_timestamp($app_end_time) . "\n";
   print {$sta_glo_fil_handle} "   it took $runtime_minutes minutes to complete.\n\n";
   close $sta_glo_fil_handle;
}
if (!$out_par_opt_quiet) {
   print 'processing completed ' . format_timestamp($app_end_time) . "\n";
   print "   it took $runtime_minutes minutes to complete.\n\n";
}

__END__


=head1 OPTIONS

Please be aware that it is allowed to shorten parameter names as long as they
remain unique. So do not bother to write --dumplines, shorten it according to
your liking, too short could make it difficult to read. --dump 5 for example
could do.

=over 8

=item B<--help>
Show the short help summary and end the program.

=item B<--manual>
Show the long help information and end the program.

=item B<--version>
Show the various version numbers and end the program.

=item B<--optionfile>
If you specify an option file this file contains the command-line parameters
to be used in stead of the actual parameters. Should be the only parameter,
any other parameter will be ignored.

=item B<--quiet>
No information as console output, problems continue to be reported.

=item B<--verbose>
More information as console output, if interactively run helps to keep track of things.

=item B<--dumplines i>
Integer (i), number of lines from the input file to show on the console to track processing
or be informed about file content.

=item B<--logconfig filename>
File to be used to configure logging. If nothing 
specified './config/logging.config' will be used.

=item B<--loglevel fatal|error|warn|info|debug  |  --debug  |  --info>
Defines to what extend the logging should be recorded. Will overwrite 
specification from the logging config file. --debug is equal to --loglevel 
debug, --info is equal to --loglevel info.

=item B<--logger name>
Logging configuration file specifies what categories (loggers) are
available. If needed the default name can be overwritten here. 
Make sure anything specified here matches your logging configuration.

=item B<--logwatch seconds  |  watch seconds>
Logging configuration files are re-read after some seconds to identify
any changes applied. Specify number of seconds between re-reads. Defaults
to 60 seconds.

=item B<--sql  |  --nosql>
If active (default) a corresponding create table sql file is
generated for each file based on the file content and other parameters.

=item B<--csv  |  --nocsv>
If active (default) a corresponding standard .csv file is generated for 
each .tsv file.

=item B<--adult  |  --noadult>
Filtering out titles that are marked as adult content. This leads to 
inconsistencies between the files as not all records have this marker.
If --noadult is in use (default) make sure you set the import to ignore
records with foreign keys that are not satisfied and/or make sure when
working with the tables that this leads to no problems. If in doubt 
activate --adult to get consitent content. Be aware that --noadult does
not filter out all adult content. 

=item B<--config filename  |  --yaml filename>
Name of the .yaml configuration file to use. options 'config' and 'yaml' are synonyms. If not specified the default
file './config/imdb_config.yaml' will be used.

=item B<--filename filename>
Filename of an IMDb .tsv file to be analysed and processed.

=item B<--tablename tablename>
Tablename to be used in the sql create table. If not specified
will be based on the filename.

=item B<--comment "comment string to use">
Comment string to be added to the sql create table.

=item B<--primarykey column-name | (column-name, column-name ...)>
Colum names to be added to the primary key definition of the table.

=item B<--foreignkey foreign-key-expression>
Text will be passed on to a foreign key definition in the sql create table.

=item B<--locale locale_name>
Locale to specify how the numbers in the statistics should be formatted. Can 
have values like en-us, de-de, de-ch etc.

=item B<--datadirectory directory_name>
Relative or absolute path to be used to read/write data files.

=back

=cut


=head1 EXAMPLES

#TODO place usage examples here

=cut


