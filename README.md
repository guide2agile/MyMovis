# MyMovis
Manage a large number of media files

This currently is in a very raw state. So do not expect everything to run 
smoothly, probably not even run at all.

What I currently work on as a first step is to be able to download the 
official IMDb offline files to have a reference base to continue my work.

To create the jobs I currently use PERL to be very focused on data 
transformation (standard modules for csv handling) and be pretty 
platform independent. I am not saying this is the only or best approach,
it just makes sense from my past experience.

What I would like to achieve
- job to download IMDb files from the website.
- job to analyse the files and create a data-model off it.
- be able to convert the .tsv (tab separate values) file from IMDb that have a
  pretty non-standard structure to RFC compliant standard .csv files.
- able to load the .csv files to a relational DB, currently I am on MariaDB.