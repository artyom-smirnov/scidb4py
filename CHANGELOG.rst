Changelog
---------

0.0.6
~~~~~
* Fixed bug #15 (Large chunk can not fit single network message)
* Fixed reading strings greater than 8 bytes from RLE chunks
* Added 'active' property to Connection which indicate active query
* Basic query autocomplete and autocancel. Complete by default.

0.0.5
~~~~~
* Fixed bug #14 (Strange bzipped chunk issue)
* Fixed bug #5 (Compressed chunk support)
* scidb4py now provide __version__ string
