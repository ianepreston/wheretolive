"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """Where to Live."""


if __name__ == "__main__":
    main(prog_name="wheretolive")  # pragma: no cover
