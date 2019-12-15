:: WINDOWS BATCH FILE

IF NOT EXIST ..\..\batch\IMDb_download.pl GOTO :WRONGDIR

:TEST
PUSHD ..\..\batch
perl .\IMDb_download.pl --option ..\testing\IMDb_download\IMDb_download.test001.optionfile
ECHO ERRORLEVEL = %ERRORLEVEL%
POPD
GOTO :ENDHERE

:WRONGDIR
ECHO you are in the wrong directory, to run this you must be in the [your project root]\tests\IMDb_download directory

:ENDHERE