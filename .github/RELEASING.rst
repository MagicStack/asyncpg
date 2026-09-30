Releasing asyncpg
=================

The ``Release`` workflow builds and tests the source distribution and the
Linux, macOS, and Windows wheel matrix, and builds the documentation. A live
release also publishes the docs, merges and tags the release PR, creates a
GitHub release, and uploads the distributions to PyPI.

Dry-run a release
-----------------

Use either trigger to run the same builds without publishing anything:

* Add the ``release-dry-run`` label to a PR targeting ``master``. The label
  stays in effect for later commits to that PR, including commits that do not
  change ``asyncpg/_version.py``. It also works for PRs from forks. Changes to
  other labels do not trigger a dry run.

* On GitHub, open **Actions > Release > Run workflow**, then select a
  repository branch, including ``master``. Manual runs are always dry runs.
  GitHub shows the **Run workflow** button once this workflow is on the
  default branch.

Find the ``dist`` and ``docs-preview`` downloads under **Artifacts** on the
workflow run page. The wheels are built and tested by cibuildwheel. The
``docs-preview`` artifact contains the built HTML documentation.
Dry runs create a local documentation commit and a signed release tag on the
selected commit with a disposable signing key. They log those local refs but
do not push either one to GitHub.

Removing ``release-dry-run`` does not start a live release. Later commits to
an unlabeled version PR follow the normal release process. If a live release
run has already started, adding the label does not cancel that run; cancel it
separately in Actions if needed.

Publish a release
-----------------

1. Update ``__version__`` in ``asyncpg/_version.py`` and prepare the release
   changelog. ``.github/release_log.py`` can help gather changes since the
   previous release tag::

      $ .github/release_log.py <previously-released-version-tag>

2. Open a PR to ``master`` with the version change. Without the dry-run label,
   the ``Release`` workflow builds and tests the distributions and
   documentation. Check the build results, then have a Release Manager approve
   the pending ``pypi`` deployment using **Review deployments** on the
   workflow run.

3. After deployment approval, the workflow updates ``gh-pages``, merges and
   tags the PR, creates the GitHub release, and uploads the distributions to
   PyPI. Check these outputs after it finishes, then edit the GitHub release
   notes as needed.

4. Open ``master`` for development by updating ``asyncpg/_version.py`` to the
   next development version with a ``.dev0`` suffix.
