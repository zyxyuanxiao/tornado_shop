# -*- coding: utf-8 -*-
# @Author: YouShaoPing
# @Date:   2019-01-22 11:08:54
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2019-01-22 11:10:39

from __future__ import unicode_literals

import logging
from operator import methodcaller
import asyncio

from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import BulkIndexError, expand_action, _chunk_actions
from elasticsearch.compat import map

logger = logging.getLogger('elasticsearch.helpers')


async def _process_bulk_chunk(client, bulk_actions, bulk_data, raise_on_exception=True, raise_on_error=True, *args, **kwargs):
    """
    Send a bulk request to elasticsearch and process the output.
    """
    # if raise on error is set, we need to collect errors per chunk before raising them
    errors = []

    try:
        # send the actual request
        resp = await client.bulk('\n'.join(bulk_actions) + '\n', *args, **kwargs)
    except TransportError as e:
        # default behavior - just propagate exception
        if raise_on_exception:
            raise e

        # if we are not propagating, mark all actions in current chunk as failed
        err_message = str(e)
        exc_errors = []

        for data in bulk_data:
            # collect all the information about failed actions
            op_type, action = data[0].copy().popitem()
            info = {"error": err_message, "status": e.status_code, "exception": e}
            if op_type != 'delete':
                info['data'] = data[1]
            info.update(action)
            exc_errors.append({op_type: info})

        # emulate standard behavior for failed actions
        if raise_on_error:
            raise BulkIndexError('%i document(s) failed to index.' % len(exc_errors), exc_errors)
        else:
            for err in exc_errors:
                yield False, err
            return

    # go through request-reponse pairs and detect failures
    for data, (op_type, item) in zip(bulk_data, map(methodcaller('popitem'), resp['items'])):
        ok = 200 <= item.get('status', 500) < 300
        if not ok and raise_on_error:
            # include original document source
            if len(data) > 1:
                item['data'] = data[1]
            errors.append({op_type: item})

        if ok or not errors:
            # if we are not just recording all errors to be able to raise
            # them all at once, yield items individually
            yield ok, {op_type: item}

    if errors:
        raise BulkIndexError('%i document(s) failed to index.' % len(errors), errors)


async def streaming_bulk(client, actions, chunk_size=500, max_chunk_bytes=100 * 1024 * 1024,
                         raise_on_error=True, expand_action_callback=expand_action,
                         raise_on_exception=True, max_retries=0, initial_backoff=2,
                         max_backoff=600, yield_ok=True, *args, **kwargs):
    """
    Streaming bulk consumes actions from the iterable passed in and yields
    results per action. For non-streaming usecases use
    :func:`~elasticsearch.helpers.bulk` which is a wrapper around streaming
    bulk that returns summary information about the bulk operation once the
    entire input is consumed and sent.

    If you specify ``max_retries`` it will also retry any documents that were
    rejected with a ``429`` status code. To do this it will wait (**by calling
    time.sleep which will block**) for ``initial_backoff`` seconds and then,
    every subsequent rejection for the same chunk, for double the time every
    time up to ``max_backoff`` seconds.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterable containing the actions to be executed
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
    :arg raise_on_error: raise ``BulkIndexError`` containing errors (as `.errors`)
        from the execution of the last chunk when some occur. By default we raise.
    :arg raise_on_exception: if ``False`` then don't propagate exceptions from
        call to ``bulk`` and just report the items that failed as failed.
    :arg expand_action_callback: callback executed on each action passed in,
        should return a tuple containing the action line and the data line
        (`None` if data line should be omitted).
    :arg max_retries: maximum number of times a document will be retried when
        ``429`` is received, set to 0 (default) for no retries on ``429``
    :arg initial_backoff: number of seconds we should wait before the first
        retry. Any subsequent retries will be powers of ``initial_backoff *
        2**retry_number``
    :arg max_backoff: maximum number of seconds a retry will wait
    :arg yield_ok: if set to False will skip successful documents in the output
    """
    actions = map(expand_action_callback, actions)

    for bulk_data, bulk_actions in _chunk_actions(actions, chunk_size,
                                                  max_chunk_bytes,
                                                  client.transport.serializer):

        for attempt in range(max_retries + 1):
            to_retry, to_retry_data = [], []
            if attempt:
                await asyncio.sleep(min(max_backoff, initial_backoff * 2 ** (attempt - 1)))

            try:
                async for ok, info in _process_bulk_chunk(client, bulk_actions, bulk_data,
                                                          raise_on_exception,
                                                          raise_on_error, *args, **kwargs):
                    if not ok:
                        action, info = info.popitem()
                        # retry if retries enabled, we get 429, and we are not
                        # in the last attempt
                        if max_retries \
                                and info['status'] == 429 \
                                and (attempt + 1) <= max_retries:
                            # _process_bulk_chunk expects strings so we need to
                            # re-serialize the data
                            to_retry.extend(map(client.transport.serializer.dumps, bulk_data))
                            to_retry_data.append(bulk_data)
                        else:
                            yield ok, {action: info}
                    elif yield_ok:
                        yield ok, info

            except TransportError as e:
                # suppress 429 errors since we will retry them
                if attempt == max_retries or e.status_code != 429:
                    raise
            else:
                if not to_retry:
                    break
                # retry only subset of documents that didn't succeed
                bulk_actions, bulk_data = to_retry, to_retry_data


async def bulk(client, actions, stats_only=False, *args, **kwargs):
    """
    Helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
    a more human friendly interface - it consumes an iterator of actions and
    sends them to elasticsearch in chunks. It returns a tuple with summary
    information - number of successfully executed actions and either list of
    errors or number of errors if ``stats_only`` is set to ``True``. Note that
    by default we raise a ``BulkIndexError`` when we encounter an error so
    options like ``stats_only`` only apply when ``raise_on_error`` is set to
    ``False``.

    When errors are being collected original document data is included in the
    error dictionary which can lead to an extra high memory usage. If you need
    to process a lot of data and want to ignore/collect errors please consider
    using the :func:`~elasticsearch.helpers.streaming_bulk` helper which will
    just return the errors and not store them in memory.


    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterator containing the actions
    :arg stats_only: if `True` only report number of successful/failed
        operations instead of just number of successful and a list of error responses

    Any additional keyword arguments will be passed to
    :func:`~elasticsearch.helpers.streaming_bulk` which is used to execute
    the operation, see :func:`~elasticsearch.helpers.streaming_bulk` for more
    accepted parameters.
    """
    success, failed = 0, 0

    # list of errors to be collected is not stats_only
    errors = []

    # make streaming_bulk yield successful results so we can count them
    kwargs['yield_ok'] = True
    async for ok, item in streaming_bulk(client, actions, *args, **kwargs):
        # go through request-reponse pairs and detect failures
        if not ok:
            if not stats_only:
                errors.append(item)
            failed += 1
        else:
            success += 1

    return success, failed if stats_only else errors


async def scan(client, query=None, scroll='5m', raise_on_error=True,
               preserve_order=False, size=1000, request_timeout=None, clear_scroll=True,
               scroll_kwargs=None, **kwargs):
    """
    Simple abstraction on top of the
    :meth:`~elasticsearch.Elasticsearch.scroll` api - a simple iterator that
    yields all hits as returned by underlining scroll requests.

    By default scan does not return results in any pre-determined order. To
    have a standard order in the returned documents (either by score or
    explicit sort definition) when scrolling, use ``preserve_order=True``. This
    may be an expensive operation and will negate the performance benefits of
    using ``scan``.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    :arg raise_on_error: raises an exception (``ScanError``) if an error is
        encountered (some shards fail to execute). By default we raise.
    :arg preserve_order: don't set the ``search_type`` to ``scan`` - this will
        cause the scroll to paginate with preserving the order. Note that this
        can be an extremely expensive operation and can easily lead to
        unpredictable results, use with caution.
    :arg size: size (per shard) of the batch send at each iteration.
    :arg request_timeout: explicit timeout for each call to ``scan``
    :arg clear_scroll: explicitly calls delete on the scroll id via the clear
        scroll API at the end of the method on completion or error, defaults
        to true.
    :arg scroll_kwargs: additional kwargs to be passed to
        :meth:`~elasticsearch.Elasticsearch.scroll`

    Any additional keyword arguments will be passed to the initial
    :meth:`~elasticsearch.Elasticsearch.search` call::

        scan(es,
            query={"query": {"match": {"title": "python"}}},
            index="orders-*",
            doc_type="books"
        )

    """
    scroll_kwargs = scroll_kwargs or {}

    if not preserve_order:
        query = query.copy() if query else {}
        query["sort"] = "_doc"
    # initial search
    resp = await client.search(body=query, scroll=scroll, size=size,
                               request_timeout=request_timeout, **kwargs)

    scroll_id = resp.get('_scroll_id')
    if scroll_id is None:
        return

    try:
        first_run = True
        while True:
            # if we didn't set search_type to scan initial search contains data
            if first_run:
                first_run = False
            else:
                resp = await client.scroll(scroll_id, scroll=scroll,
                                           request_timeout=request_timeout,
                                           **scroll_kwargs)

            for hit in resp['hits']['hits']:
                yield hit

            # check if we have any errrors
            if resp["_shards"]["successful"] < resp["_shards"]["total"]:
                logging.warning(
                    'Scroll request has only succeeded on %d shards out of %d.',
                    resp['_shards']['successful'], resp['_shards']['total']
                )
                if raise_on_error:
                    raise Exception(
                        scroll_id,
                        'Scroll request has only succeeded on %d shards out of %d.' %
                        (resp['_shards']['successful'], resp['_shards']['total'])
                    )

            scroll_id = resp.get('_scroll_id')
            # end of scroll
            if scroll_id is None or not resp['hits']['hits']:
                break
    finally:
        if scroll_id and clear_scroll:
            client.clear_scroll(body={'scroll_id': [scroll_id]}, ignore=(404, ))
