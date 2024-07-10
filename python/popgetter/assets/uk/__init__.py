from __future__ import annotations

from pathlib import Path

from . import (
    united_kingdom,  # noqa: F401
)

# from .united_kingdom import country, asset_prefix

uk_venv_path: str = str((Path(__file__).parent.parent / "uk_venv").absolute())


####################
# Remove the following code block
# For now-just to check the list of assets
####################
# @asset(key_prefix="uk", name="create_custom_venv")
# def create_custom_venv(
#     context,
#     pipes_subprocess_client: PipesSubprocessClient,
# ) -> Output:
#     context.log.info(
#         "Creating custom venv for UK at: %s",
#         uk_venv_path,
#     )
#     context.add_output_metadata(
#         metadata={
#             "uk_venv_path": uk_venv_path,
#             "python_version": shutil.which("python"),
#         }
#     )
#     cmd = [shutil.which("python"), "-m", "venv", uk_venv_path]

#     pcci = pipes_subprocess_client.run(command=cmd, context=context)
#     context.log.debug("pcci: %s", pcci)
#     context.log.debug("dir(pcci): %s", dir(pcci))
#     # return pcci.get_results()

#     py_exe = str(Path(uk_venv_path) / "bin" / "python")
#     context.log.info("Installing custom dependencies for UK at %s", uk_venv_path)
#     context.add_output_metadata(
#         metadata={
#             "py_exe": py_exe,
#             "python_version": shutil.which("python"),
#         }
#     )
#     cmd = [
#         py_exe,
#         "-m",
#         "pip",
#         "install",
#         "-r",
#         file_relative_path(__file__, "requirements-non-foss-uk.txt"),
#     ]
#     pcci = pipes_subprocess_client.run(command=cmd, context=context)
#     context.log.debug("pcci: %s", pcci)
#     context.log.debug("dir(pcci): %s", dir(pcci))
#     return Output(
#         value=str(pcci.get_results()),
#     )


# @asset(key_prefix="uk", name="install_mapshaper")
# def install_mapshaper(
#     context,
#     pipes_subprocess_client: PipesSubprocessClient,
# ) -> Output:
#     """
#     Assumption: `npm` is installed and on the path.
#     """
#     context.log.info("Installing Mapshaper")

#     cmd = [shutil.which("npm"), "install", "mapshaper"]
#     context.log.info("cmd: %s", cmd)

#     pcci = pipes_subprocess_client.run(
#         command=cmd, context=context, env={"PATH": os.environ["PATH"]}
#     )

#     context.log.debug("pcci: %s", pcci)
#     context.log.debug("dir(pcci): %s", dir(pcci))
#     result = pcci.get_results()
#     context.log.debug("result: %s", result)
#     context.log.debug("dir(result): %s", dir(result))
#     return Output(
#         value=str(pcci.get_results()),
#     )


# @asset(
#     key_prefix="uk",
#     name="legacy_asset",
#     deps=[create_custom_venv, install_mapshaper],
# )
# def legacy_asset(context, pipes_subprocess_client: PipesSubprocessClient) -> Output:
#     py_exe = str(Path(uk_venv_path) / "bin" / "python")
#     cmd = [py_exe, file_relative_path(__file__, "legacy/england.py")]
#     return Output(
#         value=str(
#             pipes_subprocess_client.run(
#                 command=cmd, context=context, env={"PATH": os.environ["PATH"]}
#             ).get_results()
#         )
#     )
