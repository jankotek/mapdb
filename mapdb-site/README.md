This is www.mapdb.org website and documentation. It uses Sphinx to generate HTML, PDF and ebook.

To compile install newest Sphinx, version bundled with Ubuntu 14.04 is too old:

    sudo apt-get install python-pip
    sudo pip install sphinx
    sudo pip install ablog

To generate PDF one has to install texlive packages.

    sudo apt-get install texlive-latex-base texlive-fonts-recommended texlive-latex-extra

Than run

    ./pdf


And generate html. It depends on PDF and MapDB source code. Result will be placed in _build/html

    make clean html

To publish local changes into Github run publish. It will ask for
github login:

    ./publish


