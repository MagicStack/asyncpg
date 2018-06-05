Releasing asyncpg
=================

When making an asyncpg release follow the below checklist.

1. Remove the ``.dev0`` suffix from ``__version__`` in ``asyncpg/__init__.py``.

2. Make a release commit:

   .. code-block:: shell

      $ git commit -a -m "asyncpg vX.Y.0"

   Here, X.Y.0 is the ``__version__`` in ``asyncpg/__init__.py``.

3. Force push into the "releases" branch on Github:

   .. code-block:: shell

      $ git push --force origin master:releases

4. Wait for CI to make the release build.  If there are errors,
   investigate, fix and repeat steps 2 through 4.

5. Prepare the release changelog by cleaning and categorizing the output of
   ``.github/release_log.py``.  Look at previous releases for examples
   of changelog formatting:

   .. code-block:: shell

      $ .github/release_log.py <previously-released-version-tag>

6. Make an annotated, signed git tag and use the changelog as the tag
   annotation:

   .. code-block:: shell

      $ git tag -s vX.Y.0
      <paste changelog>

7. Push the release commit and the new tag to master on Github:

   .. code-block:: shell

      $ git push --follow-tags

8. Wait for CI to publish the build to PyPI.

9. Edit the release on Github and paste the same content you used for
   the tag annotation (Github treats tag annotations as plain text,
   rather than Markdown.)

10. Open master for development by bumping the minor component of
    ``__version__`` in ``asyncpg/__init__.py`` and appending the ``.dev0``
    suffix.
