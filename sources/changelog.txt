=========
Changelog
=========

Changes in v0.2.1
=================
- Enable use of ``HotQueue.worker()`` decorator without any keyword arguments, like ``@queue.worker``

Changes in v0.2.0
=================
- Renamed ``dequeue()`` method on ``HotQueue`` to ``get()``
- Renamed ``enqueue()`` method on ``HotQueue`` to ``put()``
- Added ``HotQueue.worker()`` function decorator
- ``HotQueue.get()`` method now supports block and timeout arguments
- Added test suite

Changes in v0.1
===============
- Initial release