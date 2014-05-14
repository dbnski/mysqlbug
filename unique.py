#!/usr/bin/python

import time
import random
import multiprocessing
import Queue
import MySQLdb

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
        delay_queue = Queue.Queue(5)

        q1 = multiprocessing.JoinableQueue()
        q2 = multiprocessing.JoinableQueue()

        w1 = InsertWorker(q1)
        w1.start()

        w2 = InsertWorker(q2)
        w2.start()
        
        while True:
            row_id = self.queue.get()
            if row_id == 0:
              self.queue.task_done()
              break
            try:
                cursor = self.db.cursor()
                cursor.execute('delete from test where v1 = %d' % (row_id))
                cursor.close()

                q1.put(row_id)
                q2.put(row_id)
                q2.join()
                q1.join()
            except:
                pass
            self.queue.task_done()

        q1.put(0)
        w1.join()
        q2.put(0)
        w2.join()


class InsertWorker (DatabaseWorker):

    def task(self):
        while True:
            row_id = self.queue.get()
            if row_id == 0:
              self.queue.task_done()
              break
            try:
                cursor = self.db.cursor()
                cursor.execute('insert into test (v1, v2) values (%d, %d)' % (row_id, 8));
                cursor.close()
            except:
                pass
            self.queue.task_done()


random.seed()

q = multiprocessing.JoinableQueue()
w = DeleteWorker(q)
w.start()

while True:
  i = random.randint(1, 2522494)
  q.put(i)
  q.join()

q.put(0)
w.join()
