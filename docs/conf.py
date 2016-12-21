#!/usr/bin/env python3

import alabaster
import os
import re
import sys

sys.path.insert(0, os.path.abspath('..'))

with open(os.path.abspath('../setup.py'), 'rt') as f:
    _m = re.search(
        r'''VERSION\s*=\s*(?P<q>'|")(?P<ver>[\d\.]+)(?P=q)''', f.read())
    if not _m:
        raise RuntimeError('unable to read the version from setup.py')
    version = _m.group('ver')


# -- General configuration ------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.viewcode',
    'sphinx.ext.githubpages',
    'sphinxcontrib.asyncio',
]

add_module_names = False

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = 'asyncpg'
copyright = '2016-present, the ayncpg authors and contributors'
author = '<See AUTHORS file>'
release = version
language = None
exclude_patterns = ['_build']
pygments_style = 'sphinx'
todo_include_todos = False
suppress_warnings = ['image.nonlocal_uri']

# -- Options for HTML output ----------------------------------------------

html_theme = 'sphinx_rtd_theme'
# html_theme_options = {
#     'description': 'asyncpg is a fast PostgreSQL client library for the '
#                    'Python asyncio framework',
#     'show_powered_by': False,
# }
html_theme_path = [alabaster.get_path()]
html_title = 'asyncpg Documentation'
html_short_title = 'asyncpg'
html_static_path = []
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
    ]
}
html_show_sourcelink = False
html_show_sphinx = False
html_show_copyright = True
htmlhelp_basename = 'asyncpgdoc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {}

latex_documents = [
    (master_doc, 'asyncpg.tex', 'asyncpg Documentation',
     author, 'manual'),
]


# -- Options for manual page output ---------------------------------------

man_pages = [
    (master_doc, 'asyncpg', 'asyncpg Documentation',
     [author], 1)
]


# -- Options for Texinfo output -------------------------------------------

texinfo_documents = [
    (master_doc, 'asyncpg', 'asyncpg Documentation',
     author, 'asyncpg',
     'asyncpg is a fast PostgreSQL client library for the '
     'Python asyncio framework',
     'Miscellaneous'),
]
