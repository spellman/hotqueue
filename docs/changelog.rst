=========
Changelog
=========

Changes in v0.2.5
=================
- Fixed a bug in v0.2.4 that prevented install in some environments

Changes in v0.2.4
=================
- ``HotQueue.worker`` decorator method can now be used to decorate a class method

Changes in v0.2.3
=================
- Added support for custom serialization (JSON, etc)

Changes in v0.2.2
=================
- Added ``key_for_name`` function

Changes in v0.2.1
=================
- ``HotQueue.worker`` decorator method can now be used without any keyword arguments

Changes in v0.2.0
=================
- Renamed ``HotQueue.dequeue`` method to ``get``
- Renamed ``HotQueue.enqueue`` method to ``put``
- Added ``HotQueue.worker`` decorator method
- ``HotQueue.get`` method now supports block and timeout arguments
- Added test suite

Changes in v0.1.0
=================
- Initial release