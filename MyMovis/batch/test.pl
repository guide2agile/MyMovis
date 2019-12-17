#!/usr/bin/perl

=head1 NAME

IMDb_download - Program to download and unzip offline files from IMDb website

=head1 SYNOPSIS

  IMDb_download [options]
  
    --option <filename>      Read the options from a file, not commandline
    --source <web-page>      What web-page to use
    --target <directory>     Directory to be used as target for the download
    --file <filename>        Name of a .gz to download e.g. title.akas.tsv.gz
                             file parameter can be repeated multiple times
    --comment "qoted string" Comment to be noted for the file (one per file)
    --quiet | noquiet        No output to standard output
    --verbose | noverbose    Extended processing information on standard output
    --help                   Show this help information
    --manual                 Manual, the more elaborate help
    --version                Show version number
    
  Defaults are, if parameters are omitted: 
    --source https://datasets.imdbws.com/
    --target ../data/imdb/
    --file title.ratings.tsv.gz
    --comment "rating and votes information for IMDb titles"
    --file title.episode.tsv.gz
    --comment "episode information for tv series"
    --file title.basics.tsv.gz
    --comment "titles in internet movie database (IMDb)"
    --file title.akas.tsv.gz
    --comment "alternatives for IMDb titles (also know as - AKA)"
    --file title.crew.tsv.gz
    --comment "director and writer information for all IMDb titles"
    --file title.principals.tsv.gz
    --comment "principal cast/crew for IMDb titles"
    --file name.basics.tsv.gz
    --comment "names in internet movie database (IMDb)"
    --noquiet
    --noverbose
=cut

=head1 DESCRIPTION

  Program to download the Internet Movie Database (IMDb) offline files. 
  Downloaded from https://datasets.imdbws.com/
 
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

our $VERSION = 0.7;

use strict;
use warnings;

# use the helper subroutines that do downloads, logging, etc.
use lib './lib';    ## There might be a problem in eclipse with this, read my comments in the wiki
use MovisHelper;

##### MAIN #####

#getting all that configuration values to be used and start logging
read_parameters();
my $logger = inform_start();
read_configfile();

# process list of files to download, taking the defaults from MovisHelper
while ( my $file_to_download = shift @download_file_list ) {
   file_download_unzip( $download_source_uri, $file_to_download, $app_data_dir, $download_file_comment{$file_to_download} );
}

# mark completion and exit with success
inform_end $logger, 0;

__END__

=head1 EXIT STATUS

  Exits with a return value of 0 in case of complete success.
  
  Error exit states are:
  
  Warning exit states are:
  
=cut

#TODO PERLDOC missing sections EXAMPLES |   | REQUIRED ARGUMENTS | OPTIONS |
#TODO                          DIAGNOSTICS | CONFIGURATION | DEPENDENCIES |
#TODO                          INCOMPATABILITIES | BUGS AND LIMITATIONS"

#TODO Error handling and exit states
#TODO in case of warnings correct exit-state
#TODO error message when croaking and setting the exit-state
