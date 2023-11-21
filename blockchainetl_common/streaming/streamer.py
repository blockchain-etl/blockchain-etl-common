# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import logging
import os
import time

from blockchainetl_common.streaming.streamer_adapter_stub import StreamerAdapterStub
from blockchainetl_common.file_utils import smart_open


class Streamer:
    def __init__(
            self,
            blockchain_streamer_adapter=StreamerAdapterStub(),
            last_synced_block_file='last_synced_block.txt',
            lag=0,
            start_block=None,
            end_block=None,
            period_seconds=10,
            block_batch_size=10,
            ramp_up_blocks=0,
            retry_errors=True,
            pid_file=None):
        """
        Initializes a new instance of the Streamer class.

        Parameters:
        - blockchain_streamer_adapter (StreamerAdapter, optional): Adapter for interacting with the blockchain. Default is StreamerAdapterStub().
        - last_synced_block_file (str, optional): Path to the file storing the last synced block number. Default is 'last_synced_block.txt'.
        - lag (int, optional): Number of blocks to stay behind the current block. Default is 0.
        - start_block (int, optional): The starting block number for syncing. Default is None.
        - end_block (int, optional): The ending block number for syncing. Default is None.
        - period_seconds (int, optional): Time interval in seconds between sync cycles. Default is 10.
        - block_batch_size (int, optional): Number of blocks to process per batch. Default is 10.
        - ramp_up_blocks (int, optional): Specifies the count of initial blocks to be processed one by one before switching to batch processing. This approach is particularly beneficial for mitigating issues like Out-of-Memory (OOM) errors when dealing with large batches. Default is 0.
        - retry_errors (bool, optional): Whether to retry on errors during syncing. Default is True.
        - pid_file (str, optional): File path for writing the process ID. Default is None.

        Description:
        This method sets up a new Streamer instance for blockchain data streaming. It configures the blockchain adapter, file paths, and synchronization parameters. The single_block_ramp_up_count parameter enables initial processing of blocks individually, allowing for a smooth transition to batch processing. The method also handles the initialization of the last synced block file, taking into account the provided start_block parameter and the existence of the last synced block file.
        """
        self.blockchain_streamer_adapter = blockchain_streamer_adapter
        self.last_synced_block_file = last_synced_block_file
        self.lag = lag
        self.start_block = start_block
        self.end_block = end_block
        self.period_seconds = period_seconds

        self.block_batch_size = block_batch_size
        self.ramp_up_blocks = ramp_up_blocks
        self.processed_blocks_count = 0

        self.retry_errors = retry_errors
        self.pid_file = pid_file

        if self.start_block is not None or not os.path.isfile(self.last_synced_block_file):
            init_last_synced_block_file((self.start_block or 0) - 1, self.last_synced_block_file)

        self.last_synced_block = read_last_synced_block(self.last_synced_block_file)

    def stream(self):
        try:
            if self.pid_file is not None:
                logging.info('Creating pid file {}'.format(self.pid_file))
                write_to_file(self.pid_file, str(os.getpid()))
            self.blockchain_streamer_adapter.open()
            self._do_stream()
        finally:
            self.blockchain_streamer_adapter.close()
            if self.pid_file is not None:
                logging.info('Deleting pid file {}'.format(self.pid_file))
                delete_file(self.pid_file)

    def _do_stream(self):
        while True and (self.end_block is None or self.last_synced_block < self.end_block):
            synced_blocks = 0

            try:
                synced_blocks = self._sync_cycle()
            except Exception as e:
                # https://stackoverflow.com/a/4992124/1580227
                logging.exception('An exception occurred while syncing block data.')
                if not self.retry_errors:
                    raise e

            if synced_blocks <= 0:
                logging.info('Nothing to sync. Sleeping for {} seconds...'.format(self.period_seconds))
                time.sleep(self.period_seconds)

    def _sync_cycle(self):
        current_block = self.blockchain_streamer_adapter.get_current_block_number()

        if self.ramp_up_blocks and self.ramp_up_blocks > 0 and self.processed_blocks_count <= self.ramp_up_blocks:
            block_batch_size = 1
        else:
            block_batch_size = self.block_batch_size

        target_block = self._calculate_target_block(current_block, self.last_synced_block, block_batch_size)
        blocks_to_sync = max(target_block - self.last_synced_block, 0)

        logging.info('Current block {}, target block {}, last synced block {}, blocks to sync {}'.format(
            current_block, target_block, self.last_synced_block, blocks_to_sync))

        if blocks_to_sync != 0:
            self.blockchain_streamer_adapter.export_all(self.last_synced_block + 1, target_block)
            logging.info('Writing last synced block {}'.format(target_block))
            write_last_synced_block(self.last_synced_block_file, target_block)
            self.last_synced_block = target_block

            self.processed_blocks_count += blocks_to_sync

        return blocks_to_sync

    def _calculate_target_block(self, current_block, last_synced_block, block_batch_size):
        target_block = current_block - self.lag
        target_block = min(target_block, last_synced_block + block_batch_size)
        target_block = min(target_block, self.end_block) if self.end_block is not None else target_block
        return target_block


def delete_file(file):
    try:
        os.remove(file)
    except OSError:
        pass


def write_last_synced_block(file, last_synced_block):
    write_to_file(file, str(last_synced_block) + '\n')


def init_last_synced_block_file(start_block, last_synced_block_file):
    if os.path.isfile(last_synced_block_file):
        raise ValueError(
            '{} should not exist if --start-block option is specified. '
            'Either remove the {} file or the --start-block option.'
                .format(last_synced_block_file, last_synced_block_file))
    write_last_synced_block(last_synced_block_file, start_block)


def read_last_synced_block(file):
    with smart_open(file, 'r') as last_synced_block_file:
        return int(last_synced_block_file.read())


def write_to_file(file, content):
    with smart_open(file, 'w') as file_handle:
        file_handle.write(content)
