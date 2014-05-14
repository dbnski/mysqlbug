#!/usr/bin/python

import time
import random
import multiprocessing
import Queue
import MySQLdb
import optparse


class DatabaseWorker (multiprocessing.Process):

    def __init__(self, queue):
        self.queue = queue
        super(DatabaseWorker, self).__init__()

    def run(self):
        self.db = MySQLdb.connect(host='localhost', db='test', user='msandbox', passwd='msandbox', unix_socket='/tmp/mysql_sandbox5531.sock')
        self.db.autocommit(True)
        cursor = self.db.cursor()
        cursor.execute('set transaction isolation level read committed')
        cursor.close()
        self.task()
        self.db.close()

    def task(self):
        pass


class DeleteWorker (DatabaseWorker):

    def task(self):
        delay_queue = Queue.Queue()

        q1 = multiprocessing.JoinableQueue(16)
        q2 = multiprocessing.JoinableQueue(16)

        w1 = InsertWorker(q1)
        w1.start()

        w2 = InsertWorker(q2)
        w2.start()
        
        while True:
            delete_id = self.queue.get()
            if delete_id == 0:
                while not delay_queue.empty():
                    insert_id = delay_queue.get()
                    q1.put(insert_id)
                    q2.put(insert_id)
                    q2.join()
                    q1.join()
                self.queue.task_done()
                break
            delay_queue.put(delete_id)

            try:
                cursor = self.db.cursor()
                cursor.execute('delete from test where v1 = %d' % (delete_id))
                cursor.close()
            except:
                pass


            if delay_queue.qsize() > 100:
                while not delay_queue.empty():
                    insert_id = delay_queue.get()
                    q1.put(insert_id)
                    q2.put(insert_id)
                    q2.join()
                    q1.join()

            self.queue.task_done()

        q1.put(0)
        w1.join()
        q2.put(0)
        w2.join()


class InsertWorker (DatabaseWorker):

    def task(self):
        while True:
            id = self.queue.get()
            if id == 0:
                self.queue.task_done()
                break
            try:
                cursor = self.db.cursor()
                cursor.execute('insert into test (v1, v2) values (%d, %d)' % (id, 8));
                cursor.close()
            except:
                pass
            self.queue.task_done()


def cleanup():
    db = MySQLdb.connect(host='localhost', db='test', user='msandbox', passwd='msandbox', unix_socket='/tmp/mysql_sandbox5531.sock')
    db.autocommit(True)
    cursor = db.cursor()
    cursor.execute('truncate table test')
    cursor.close()
    db.close()

def prepare(size=0):
    if not size > 0:
        return

    q = multiprocessing.JoinableQueue(16)
    w = InsertWorker(q)
    w.start()

    for i in xrange(1, size):
        q.put(i)
        q.join()

    q.put(0)
    w.join()

def run(size=0):
    if not size > 0:
        return

    random.seed()

    q = multiprocessing.JoinableQueue(16)
    w = DeleteWorker(q)
    w.start()

    while True:
        i = random.randint(1, size)
        q.put(i)
        q.join()

    q.put(0)
    w.join()

def main():
    parser = optparse.OptionParser()

    parser.add_option('-c', '--cleanup',
                      action='store_true', default=False, dest='cleanup')
    parser.add_option('-p', '--prepare',
                      action='store_true', default=False, dest='prepare')
    parser.add_option('-r', '--run',
                      action='store_true', default=False, dest='run')
    parser.add_option('-s', '--size',
                      action='store', type='int', default=0, dest='size')

    (options, args) = parser.parse_args()

    if int(options.cleanup) + int(options.prepare) + int(options.run) != 1:
        print 'Please specify one of --cleanup, --prepare or --run.'
        exit(1)

    if not options.cleanup and not options.size > 0:
        print 'Pleasae specify table size.'
        exit(1)

    if options.cleanup:
        cleanup()
    if options.prepare:
        prepare(options.size)
    if options.run:
        run(options.size)

if __name__ == '__main__':
    main()
