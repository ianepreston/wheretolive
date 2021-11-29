"""Handle writing to Dropbox."""
from os import getenv
from pathlib import Path

import dropbox
from dotenv import load_dotenv

from wheretolive.logconf import get_logger


load_dotenv()
logger = get_logger(__name__)


def dropbox_save(file: Path, savepath: str):
    """Save a file to Dropbox."""
    # Check for an access token
    TOKEN = getenv("DROPBOX_KEY")
    if len(TOKEN) == 0:
        raise ValueError("Couldn't load Dropbox token from environment variable.")

    # Create an instance of a Dropbox class, which can make requests to the API.
    logger.info("Creating a Dropbox object...")
    with dropbox.Dropbox(TOKEN) as dbx:

        # Check that the access token is valid
        try:
            dbx.users_get_current_account()
        except dropbox.exceptions.AuthError as err:
            raise RuntimeError("Invalid access token; try re-generating.") from err
        with open(file, "rb") as f:
            logger.info(f"Uploading {file} to dropbox as {savepath}.")
            try:
                dbx.files_upload(
                    f.read(), savepath, mode=dropbox.files.WriteMode("overwrite")
                )
            except dropbox.exceptions.ApiError as err:
                # This checks for the specific error where a user doesn't have
                # enough Dropbox space quota to upload this file
                if (
                    err.error.is_path()
                    and err.error.get_path().reason.is_insufficient_space()
                ):
                    raise RuntimeError(
                        "ERROR: Cannot back up; insufficient space."
                    ) from err
                elif err.user_message_text:
                    raise RuntimeError(err.user_message_text) from err
                else:
                    raise RuntimeError(err) from err


if __name__ == "__main__":
    print("Just testing")
    test_file = Path(__file__).resolve().parents[2] / "data" / "2021-11-23_logs.txt"
    test_out = "/wheretolive/ian/test.txt"
    dropbox_save(test_file, test_out)
