Building project depends on "gb"

to do this, run::

	go get github.com/skelterjohn/go-gb/gb

To build::

	gb

To run::

  ./undis

.. Note::
  Make sure you don't have "gb" aliased to anything.  You can check this
  by running::

    which gb

  To fix (temporarily)::

    unialias gb
