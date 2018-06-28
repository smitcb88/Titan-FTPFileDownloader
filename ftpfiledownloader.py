"""Download file(s) from FTP and upload directly to Titan's blob storage.

For more help, please execute the following at the command prompt:
ftpfiledownloader --help

"""


import datetime
import ftplib
import posixpath  # Unix separators needed for FTP despite code potentially running on windows so use this over os.path
import re
import sys

import click


class FTPFileNotFoundError(Exception):
    """No files were found matching the provided file pattern."""


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


class TitanFlowManager(object):
    def __init__(self, ftp_connection_string, ftp_file_pattern, fetch_one, archive_folder):
        """Initialise an object that controls the flow of the application.

        Positional Arguments:
        1. ftp_connection_string (tuple): FTP connection details in the format of
        (protocol, user, pass, host, port, path) - port and path can be None
        2. ftp_file_pattern: (string): The regular expression pattern to use to locate the file(s) to download
        3. fetch_one (bool): If provided, only the most recent file that matches the pattern will be retrieved,
        otherwise all files
        4. archive_folder (string): The full path to the archive folder that the matched file(s) should be archived to.
        This folder must already exist or the move will fail

        """
        self.ftp_connection_string = ftp_connection_string
        self.ftp_file_pattern = ftp_file_pattern
        self.fetch_one = fetch_one
        self.archive_folder = archive_folder

        from datalake import utilities
        self.acquire_program = utilities.AcquireProgram()
        self.logger = self.acquire_program.logger

        self._current_file_name = None
        self._dir_details = None

    def _get_matching_files(self, ftp):
        """Return a list of file names (most recent or all determined by fetch_one behaviour) matching the pattern.

        Positional Arguments:
        1. ftp (ftplib.FTP): the live (open and connected) ftp object

        """
        self._pattern = re.compile(self.ftp_file_pattern)
        matching_files = None
        self.logger.info("Searching for matching files...")
        matching_files = None
        try:
            matching_files = {file_name: details["modified"]
                              for file_name, details in ftp.mlsd() if self._pattern.match(file_name)}
        except ftplib.error_perm:
            self._dir_details = {}
            ftp.dir(self._process_dir_output)
            matching_files = self._dir_details
        if not matching_files:
            raise FTPFileNotFoundError()
        if self.fetch_one and len(matching_files) > 1:
            return [sorted(matching_files, key=lambda x: matching_files[x], reverse=True)[0]]
        return matching_files

    def _process_dir_output(self, line):
        """Parse the output line from the FTP DIR command and add to self._dir_details {file_name: modified}.

        Positional Arguments:
        1. line (string): the line to process

        """
        columns = line.split()
        time_stamp = " ".join(columns[5:8])
        file_name = " ".join(columns[8:])
        if not self._pattern.match(file_name):
            return

        try:
            modified = datetime.datetime.strptime(time_stamp + datetime.datetime.now().strftime(" %Y"),
                                                  "%b %d %H:%M %Y")
        except ValueError:
            modified = datetime.datetime.strptime(time_stamp, "%b %d  %Y")
        self._dir_details[file_name] = modified

    def download_files(self):
        """Connect to the FTP server, find the file(s) matching the pattern and upload directly to Titan's storage."""
        protocol, user, password, host, port, path = self.ftp_connection_string
        path = posixpath.normpath(path)
        archive_path = posixpath.normpath(self.archive_folder)
        ftp_class = ftplib.FTP if protocol == "ftp" else ftplib.FTP_TLS
        self.logger.info("Connecting to the FTP server...")
        with ftp_class() as ftp:
            ftp.connect(host, port or 0)
            ftp.login(user, password)
            if protocol == "ftps":
                ftp.prot_p()
            # Ensure the archive path exists - if not, unhandled error will abort process
            ftp.cwd(archive_path)
            # Forward slash needed before the path to tell the server this is abs, not rel path
            ftp.cwd("/%s" % path)
            for file_name in self._get_matching_files(ftp):
                self.logger.info("Uploading file in chunks...")
                self._current_file_name = file_name
                ftp.retrbinary("RETR %s" % file_name, self.upload, blocksize=1250000)
                self.logger.info("Succesfully uploaded. Moving file to archive...")
                ftp.rename(posixpath.join("/", path, file_name), posixpath.join("/", archive_path, file_name))

    def run(self):
        """Run the end to end download and upload process."""
        self.logger.info("EXECUTION STARTED")
        self.logger.info("Commencing the download & upload process...")
        self.download_files()
        self.logger.info("EXECUTION FINISHED")

    def upload(self, bytes):
        """Upload the bytes by appending to a blob.

        Positional Arguments:
        1. bytes (bytes): the data to upload

        """
        blob_name = self.acquire_program.get_blob_name("{TITAN_DATA_SET_NAME}_{TITAN_LOAD_DATE}_{file_name}",
                                                       file_name=self._current_file_name)
        self.logger.info("Uploading bytes...")
        self.acquire_program.append_blob_from_bytes(bytes, blob_name=blob_name)


@click.command()
@click.option("-c", "--ftp-connection-string", type=_FTPURIType(), required=True, help="FTP connection details in the "
              "format, ftp|ftps://<user>:<pass>@<host>[:port][/path]")
@click.option("-f", "--ftp-file-pattern", required=True, help="The regular expression pattern to use to locate the "
              "file(s) to download. Before a regex match is sought, any instances of YYYY, MM and DD "
              "(case-sensitive) will be replaced by the year, month and date respectively of the --load-date.")
@click.option("-m", "--fetch-one", required=True, type=bool, help="If provided, only the most recent file that matches "
              "the pattern will be retrieved, otherwise all files.")
@click.option("-a", "--archive-folder", required=True, help="The full path to the archive folder that the matched "
              "file(s) should be archived to. This folder must already exist or the whole process will be aborted "
              "before any files are attempted to be downloaded.")
@click.option("-l", "--load-date", type=_DateType(), help="If provided, must be in the format of YYYY-MM-DD. Defaults "
              "to yesterday.")
def main(ftp_connection_string, ftp_file_pattern, fetch_one, archive_folder, load_date):
    """Download file(s) from FTP and upload directly to Titan's blob storage.

    Look for files that match the provided pattern and download either the most recent one or all, failing if there
    are no files. For more information, execute the following at the command prompt: ftpfiledownloader --help

    """
    if load_date is None:
        load_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    yyyy, mm, dd = str(load_date).split("-")
    ftp_file_pattern = ftp_file_pattern.replace("YYYY", yyyy).replace("MM", mm).replace("DD", dd)
    flow_manager = TitanFlowManager(ftp_connection_string, ftp_file_pattern, fetch_one, archive_folder)
    try:
        flow_manager.run()
    except Exception as error:
        flow_manager.logger.exception(error)
        sys.exit("ERROR ENCOUNTERED - CHECK LOGS")
