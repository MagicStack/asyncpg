#!/usr/bin/env python3

import alabaster
import os
import sys

sys.path.insert(0, os.path.abspath('..'))

version_file = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                            'asyncpg', '_version.py')

with open(version_file, 'r') as f:
    for line in f:
        if line.startswith('__version__ ='):
            _, _, version = line.partition('=')
            version = version.strip(" \n'\"")
            break
    else:
        raise RuntimeError(
            'unable to read the version from asyncpg/_version.py')

# -- General configuration ------------------------------------------------

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.viewcode',
    'sphinx.ext.githubpages',
    'sphinx.ext.intersphinx',
    'sphinxcontrib.asyncio',
]

add_module_names = False

templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
project = 'asyncpg'
copyright = '2016-present, the asyncpg authors and contributors'
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
html_static_path = ['_static']
html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
    ]
}
html_show_sourcelink = False
html_show_sphinx = False
html_show_copyright = True
html_context = {
    'css_files': [
        '_static/theme_overrides.css',
    ],
}
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

# -- Options for intersphinx ----------------------------------------------

intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
