"""Download file(s) from FTP and upload directly to Titan's blob storage.

For more help, please type the following at the command prompt:
ftpfiledownloader --help

"""


import datetime
import re

import click


class _DateType(click.ParamType):
    """Initialise a custom click type to be used to validate a date provided at command line input."""

    name = "Date (YYYY-MM-DD)"

    def convert(self, value, param, ctx):
        """Check that the input is a valid FTP connection string on a standard regex pattern.

        This method is called implicitly by click and shouldn't be invoked directly.

        Positional Arguments:
        1. value (string): the value that is to be validated / converted
        2. param (unknown): (unknown as not documented by click). This value should be passed to the second parameter of
        the fail() method
        3. ctx (unknown): (unknown as not documented by click). This value should be passed to the third parameter of
        the fail() method

        """
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            self.fail("Incorrect date format, should be YYYY-MM-DD")


class _FTPURIType(click.ParamType):
    """Initialise a custom click type to be used to validate FTP connection details provided at command line input."""

    name = "FTP Path"
    # ftp|ftps://<user>:<password>@<host>[:port][/path]
    _regex = re.compile(
        r"(?P<protocol>ftp|ftps)://(?P<user>.+):(?P<password>.+)@(?P<host>[^:/]+)(?::(?P<port>\d+))?"
        r"(?:/(?P<path>.*))?")

    def convert(self, value, param, ctx):
        """Check that the input is a valid FTP connection string on a standard regex pattern.

        This method is called implicitly by click and shouldn't be invoked directly.

        Positional Arguments:
        1. value (string): the value that is to be validated / converted
        2. param (unknown): (unknown as not documented by click). This value should be passed to the second parameter of
        the fail() method
        3. ctx (unknown): (unknown as not documented by click). This value should be passed to the third parameter of
        the fail() method

        """
        match = self._regex.match(value)
        if match is None:
            self.fail("%s is not a valid FTP URI" % value, param, ctx)
        else:
            return match.groups()


@click.command()
@click.option("-c", "--ftp-connection-string", type=_FTPURIType(), required=True, help="FTP connection details in the "
              "format, ftp|ftps://<user>:<pass>@<host>[:port][/path]")
@click.option("-f", "--ftp-file-pattern", required=True, help="The regular expression pattern to use to locate the "
              "file(s) to downloader. Before a regex match is sought, any instances of YYYY, MM and DD "
              "(case-insensitive) will be replaced by the year, month and date of the --load-date respectively.")
@click.option("-m", "--fetch-one", required=True, type=bool, help="If provided, only the most recent file that matches "
              "the pattern will be retrieved, otherwise all files.")
@click.option("-a", "--archive-folder", required=True, help="The full path to the archive folder that the downloaded "
              "file should be archived to. This folder must already exist or the move will fail.")
@click.option("-l", "--load-date", type=_DateType(), help="If provided, must be in the format of YYYY-MM-DD. Defaults "
              "to yesterday.")
def main(ftp_conection_string, ftp_file_pattern, fetch_one, archive_folder, load_date):
    """Download file(s) from FTP and upload directly to Titan's blob storage.

    Include in the help message the fact that it'll fail if there are no files.

    """