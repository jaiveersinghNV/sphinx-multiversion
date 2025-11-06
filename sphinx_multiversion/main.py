# -*- coding: utf-8 -*-
import itertools
import argparse
import multiprocessing
import concurrent.futures
import contextlib
import json
import logging
import os
import pathlib
import re
import string
import subprocess
import sys
import tempfile
import venv

from sphinx import config as sphinx_config
from sphinx import project as sphinx_project

from . import sphinx
from . import git


@contextlib.contextmanager
def working_dir(path):
    prev_cwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def load_sphinx_config_worker(q, confpath, confoverrides, add_defaults):
    try:
        with working_dir(confpath):
            current_config = sphinx_config.Config.read(
                confpath,
                confoverrides,
            )

        if add_defaults:
            current_config.add(
                "smv_tag_whitelist", sphinx.DEFAULT_TAG_WHITELIST, "html", str
            )
            current_config.add(
                "smv_branch_whitelist",
                sphinx.DEFAULT_TAG_WHITELIST,
                "html",
                str,
            )
            current_config.add(
                "smv_remote_whitelist",
                sphinx.DEFAULT_REMOTE_WHITELIST,
                "html",
                str,
            )
            current_config.add(
                "smv_latest_version",
                sphinx.DEFAULT_LATEST_VERSION,
                "html",
                str
            )
            current_config.add(
                "smv_released_pattern",
                sphinx.DEFAULT_RELEASED_PATTERN,
                "html",
                str,
            )
            current_config.add(
                "smv_outputdir_format",
                sphinx.DEFAULT_OUTPUTDIR_FORMAT,
                "html",
                str,
            )
            current_config.add("smv_prefer_remote_refs", False, "html", bool)
        current_config.pre_init_values()
        current_config.init_values()
    except Exception as err:
        q.put(err)
        return

    q.put(current_config)


def load_sphinx_config(confpath, confoverrides, add_defaults=False):
    q = multiprocessing.Queue()
    proc = multiprocessing.Process(
        target=load_sphinx_config_worker,
        args=(q, confpath, confoverrides, add_defaults),
    )
    proc.start()
    proc.join()
    result = q.get_nowait()
    if isinstance(result, Exception):
        raise result
    return result


def get_python_flags():
    if sys.flags.bytes_warning:
        yield "-b"
    if sys.flags.debug:
        yield "-d"
    if sys.flags.hash_randomization:
        yield "-R"
    if sys.flags.ignore_environment:
        yield "-E"
    if sys.flags.inspect:
        yield "-i"
    if sys.flags.isolated:
        yield "-I"
    if sys.flags.no_site:
        yield "-S"
    if sys.flags.no_user_site:
        yield "-s"
    if sys.flags.optimize:
        yield "-O"
    if sys.flags.quiet:
        yield "-q"
    if sys.flags.verbose:
        yield "-v"
    for option, value in sys._xoptions.items():
        if value is True:
            yield from ("-X", option)
        else:
            yield from ("-X", "{}={}".format(option, value))


def setup_venv(venv_path, requirements_file):
    """
    Create a virtual environment and optionally install requirements.

    Args:
        venv_path: Path where the virtual environment should be created
        requirements_file: Path to requirements.txt to install

    Returns:
        Path to the Python executable in the virtual environment
    """
    # Create virtual environment
    venv.create(venv_path, with_pip=True, clear=True)
    venv_python = os.path.join(venv_path, "bin", "python")

    # Install requirements
    subprocess.check_call(
        [venv_python, "-m", "pip", "install", "-q", "-r", requirements_file],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    return venv_python


def build_version(version_name, data, tmp, argv, args, cwd_relative):
    """
    Build documentation for a single version.

    Args:
        version_name: Name of the version to build
        data: Metadata dictionary for this version
        tmp: Temporary directory path
        argv: Base argv for sphinx-build
        args: Parsed command-line arguments
        cwd_relative: Relative path from git root to current working directory

    Returns:
        Tuple of (version_name, error_log_path, success)
    """
    # Configure version-specific logger
    logger = logging.getLogger(__name__)
    # Create a custom handler with version prefix
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            '[%(version)s] %(levelname)s: %(message)s'
        )
    )
    logger.handlers = [handler]
    logger.setLevel(logging.DEBUG)

    # Create adapter to automatically add version to all log messages
    logger = logging.LoggerAdapter(
        logger, {'version': version_name}
    )

    # Setup virtual environment for this version
    venv_path = os.path.join(tmp, "venv_{}".format(version_name))
    requirements_file = os.path.join(
        data["sourcedir"], "../requirements.txt"
    )

    logger.info(
        "Setting up environment for version: %s", version_name
    )
    try:
        venv_python = setup_venv(venv_path, requirements_file)
    except subprocess.CalledProcessError:
        logger.error(
            "Failed to setup virtual environment for %s, skipping",
            version_name
        )
        return (version_name, None, False)

    os.makedirs(data["outputdir"], exist_ok=True)

    defines = itertools.chain(
        *(
            ("-D", string.Template(d).safe_substitute(data))
            for d in args.define
        )
    )

    # Use unique error log for this version
    error_log_path = os.path.join(tmp, "smv-err-{}.log".format(version_name))

    current_argv = argv.copy()
    current_argv.extend(
        [
            *defines,
            "-D",
            "smv_current_version={}".format(version_name),
            "-w",
            error_log_path,
            data["sourcedir"],
            data["outputdir"],
            *args.filenames,
        ]
    )
    logger.debug("Running sphinx-build with args: %r", current_argv)
    cmd = (
        venv_python,
        *get_python_flags(),
        "-m",
        "sphinx",
        *current_argv,
    )
    current_cwd = os.path.join(data["basedir"], cwd_relative)
    env = os.environ.copy()
    env.update(
        {
            "SPHINX_MULTIVERSION_NAME": data["name"],
            "SPHINX_MULTIVERSION_VERSION": data["version"],
            "SPHINX_MULTIVERSION_RELEASE": data["release"],
            "SPHINX_MULTIVERSION_SOURCEDIR": data["sourcedir"],
            "SPHINX_MULTIVERSION_OUTPUTDIR": data["outputdir"],
            "SPHINX_MULTIVERSION_CONFDIR": data["confdir"],
        }
    )

    try:
        subprocess.check_call(cmd, cwd=current_cwd, env=env)
        logger.info("Successfully built version: %s", version_name)
        return (version_name, error_log_path, True)
    except subprocess.CalledProcessError as e:
        logger.error("Failed to build version %s: %s", version_name, e)
        return (version_name, error_log_path, False)


def main(argv=None):
    if not argv:
        argv = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("sourcedir", help="path to documentation source files")
    parser.add_argument("outputdir", help="path to output directory")
    parser.add_argument(
        "filenames",
        nargs="*",
        help="a list of specific files to rebuild. Ignored if -a is specified",
    )
    parser.add_argument(
        "-c",
        metavar="PATH",
        dest="confdir",
        help=(
            "path where configuration file (conf.py) is located "
            "(default: same as SOURCEDIR)"
        ),
    )
    parser.add_argument(
        "-C",
        action="store_true",
        dest="noconfig",
        help="use no config file at all, only -D options",
    )
    parser.add_argument(
        "-D",
        metavar="setting=value",
        action="append",
        dest="define",
        default=[],
        help="override a setting in configuration file",
    )
    parser.add_argument(
        "-w",
        dest="warningfile",
        help=(
            "Write warnings (and errors) to the given file, "
            "in addition to standard error."
        )
    )
    parser.add_argument(
        "--dump-metadata",
        action="store_true",
        help="dump generated metadata and exit",
    )
    parser.add_argument(
        "--additional-context",
        nargs="*",
        default=[],
        help="additional paths-from-gitroot that must be copied to build docs",
    )
    args, argv = parser.parse_known_args(argv)
    if args.noconfig:
        return 1

    # Configure logging to output to stdout/stderr
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(levelname)s: %(message)s',
        stream=sys.stdout,
    )

    logger = logging.getLogger(__name__)

    sourcedir_absolute = os.path.abspath(args.sourcedir)
    confdir_absolute = (
        os.path.abspath(args.confdir)
        if args.confdir is not None
        else sourcedir_absolute
    )

    # Conf-overrides
    confoverrides = {}
    for d in args.define:
        key, _, value = d.partition("=")
        confoverrides[key] = value

    # Parse config
    config = load_sphinx_config(
        confdir_absolute, confoverrides, add_defaults=True
    )

    # Get relative paths to root of git repository
    gitroot = pathlib.Path(
        git.get_toplevel_path(cwd=sourcedir_absolute)
    ).resolve()
    cwd_absolute = os.path.abspath(".")
    cwd_relative = os.path.relpath(cwd_absolute, str(gitroot))

    logger.debug("Git toplevel path: %s", str(gitroot))
    sourcedir = os.path.relpath(sourcedir_absolute, str(gitroot))
    logger.debug(
        "Source dir (relative to git toplevel path): %s", str(sourcedir)
    )
    if args.confdir:
        confdir = os.path.relpath(confdir_absolute, str(gitroot))
    else:
        confdir = sourcedir
    logger.debug("Conf dir (relative to git toplevel path): %s", str(confdir))
    conffile = os.path.join(confdir, "conf.py")

    # Get git references
    gitrefs = git.get_refs(
        str(gitroot),
        config.smv_tag_whitelist,
        config.smv_branch_whitelist,
        config.smv_remote_whitelist,
        files=(sourcedir, conffile),
    )

    # Order git refs
    if config.smv_prefer_remote_refs:
        gitrefs = sorted(gitrefs, key=lambda x: (not x.is_remote, *x))
    else:
        gitrefs = sorted(gitrefs, key=lambda x: (x.is_remote, *x))

    logger = logging.getLogger(__name__)

    with tempfile.TemporaryDirectory() as tmp:
        # Generate Metadata
        metadata = {}
        outputdirs = set()
        for gitref in gitrefs:
            # Clone Git repo
            repopath = os.path.join(tmp, gitref.commit)
            for dir in (cwd_relative, *args.additional_context):
                try:
                    git.copy_tree(str(gitroot), gitroot.as_uri(),
                                  repopath, gitref, dir)
                except (OSError, subprocess.CalledProcessError):
                    logger.error(
                        "Failed to copy git tree %s for %s to %s",
                        dir,
                        gitref.refname,
                        repopath,
                    )
                    continue

            # Find config
            confpath = os.path.join(repopath, confdir)
            try:
                current_config = load_sphinx_config(confpath, confoverrides)
            except (OSError, sphinx_config.ConfigError):
                logger.error(
                    "Failed load config for %s from %s",
                    gitref.refname,
                    confpath,
                )
                continue

            # Ensure that there are not duplicate output dirs
            outputdir = config.smv_outputdir_format.format(
                ref=gitref,
                config=current_config,
            )
            if outputdir in outputdirs:
                logger.warning(
                    "outputdir '%s' for %s conflicts with other versions",
                    outputdir,
                    gitref.refname,
                )
                continue
            outputdirs.add(outputdir)

            # Get List of files
            source_suffixes = current_config.source_suffix
            if isinstance(source_suffixes, str):
                source_suffixes = [current_config.source_suffix]

            current_sourcedir = os.path.join(repopath, sourcedir)
            project = sphinx_project.Project(
                current_sourcedir, source_suffixes
            )
            metadata[gitref.name] = {
                "name": gitref.name,
                "version": current_config.version,
                "release": current_config.release,
                "rst_prolog": current_config.rst_prolog,
                "is_released": bool(
                    re.match(config.smv_released_pattern, gitref.refname)
                ),
                "source": gitref.source,
                "creatordate": gitref.creatordate.strftime(sphinx.DATE_FMT),
                "basedir": repopath,
                "sourcedir": current_sourcedir,
                "outputdir": os.path.join(
                    os.path.abspath(args.outputdir), outputdir
                ),
                "confdir": confpath,
                "docnames": list(project.discover()),
            }

            if gitref.name == config.smv_latest_version:
                # Duplicate latest reference and save to root
                metadata["latest"] = metadata[gitref.name].copy()
                metadata["latest"]["name"] = "latest"
                metadata["latest"]["outputdir"] = os.path.abspath(
                    args.outputdir
                )

        if args.dump_metadata:
            print(json.dumps(metadata, indent=2))
            return 0

        if not metadata:
            logger.error("No matching refs found!")
            return 2

        # Write Metadata
        metadata_path = os.path.abspath(os.path.join(tmp, "versions.json"))
        with open(metadata_path, mode="w") as fp:
            json.dump(metadata, fp, indent=2)

        # Run Sphinx builds in parallel
        argv.extend(["-D", "smv_metadata_path={}".format(metadata_path)])

        # Determine number of parallel workers
        max_workers = min(len(metadata), os.cpu_count() or 1)
        logger.info(
            "Building %d versions in parallel with %d workers",
            len(metadata), max_workers
        )

        # Build all versions in parallel
        build_results = []
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_workers
        ) as executor:
            # Submit all build jobs
            futures = {
                executor.submit(
                    build_version,
                    version_name,
                    data,
                    tmp,
                    argv,
                    args,
                    cwd_relative
                ): version_name
                for version_name, data in metadata.items()
            }

            # Collect results as they complete
            for future in concurrent.futures.as_completed(futures):
                version_name = futures[future]
                try:
                    result = future.result()
                    build_results.append(result)
                except Exception as exc:
                    logger.error(
                        "Version %s generated an exception: %s",
                        version_name, exc
                    )
                    build_results.append((version_name, None, False))

        # Aggregate all error logs into warningfile if requested
        if args.warningfile:
            logger.info("Aggregating error logs to %s", args.warningfile)
            with open(args.warningfile, mode="w") as wf:
                for version_name, error_log_path, success in build_results:
                    if error_log_path and os.path.exists(error_log_path):
                        wf.write(
                            "=" * 70 + "\n"
                        )
                        wf.write(
                            "Version: {}\n".format(version_name)
                        )
                        wf.write(
                            "=" * 70 + "\n"
                        )
                        with open(error_log_path, mode="r") as err_log:
                            wf.write(err_log.read())
                        wf.write("\n")

        # Report final status
        successful = sum(1 for _, _, success in build_results if success)
        logger.info(
            "Build complete: %d/%d versions successful",
            successful, len(build_results)
        )

    return 0
