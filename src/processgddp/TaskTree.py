import multiprocessing
import logging

class TaskTree:
    def __init__(self, timeout=900, **poolargs):
        '''
        Class for executing a tree of tasks asyncronously.

        Runs tasks in a process pool. Tasks that are blocked by
        other tasks are added to the pool once they become unblocked.
        '''
        self._unblocked = []
        self._blocked = []
        self._inprocess = []
        self._completed = []
        self._taskFunction = {}
        self._taskRequirements = {}
        self._taskBlockedby = {}
        self._taskBlocks = {}
        self._dummyTasks = []
        self.timeout = timeout
        self.poolargs = poolargs

    def __len__(self):
        return len(self._blocked) + len(self._unblocked) + len(self._inprocess)

    def add(self, function, taskId, requires=[]):
        '''
        Add task to tree

        @params
        function     function   Function to be executed.
                                Must take at least two parameters:
                                <taskId> and a list of return values from
                                executing the tasks in <requires> will
                                always be passed as the first two parameters
                                to the function.
        string       taskId     Unique task name.
        list<string> requires   taskIds which must be completed
                                before this task can be executed.
        '''
        if self.exists(taskId):
            logging.warning("Task {} already defined".format(taskId))
            return

        self._taskFunction[taskId] = function
        self._taskRequirements[taskId] = requires
        self._taskBlockedby[taskId] = requires[:]
        if len(requires):
            self._blocked.append(taskId)
            for r in requires:
                if r in self._taskBlocks and type(self._taskBlocks[r]) is list:
                    self._taskBlocks[r].append(taskId)
                else:
                    self._taskBlocks[r] = [taskId]
        else:
            self._unblocked.append(taskId)

    def exists(self, taskId):
        return taskId in self._taskRequirements

    def build(self, *args, **kwargs):
        '''
        Execute the functions for all tasks in tree synchronously.

        Functions recive parameters as:
        <function>(<taskId>, return_values[<requires>], *args, **kwargs)
        Optional args and kwargs passed to task function.
        '''
        self._check_undefined()
        results = {}
        while len(self):
            if len(self._unblocked):
                taskId = self._pop()
                results[taskId] = self._taskFunction[taskId](
                    taskId,
                    [results[t] for t in self._taskRequirements[taskId]],
                    *args, **kwargs)
                self._complete(taskId)
            else:
                raise Exception("Tasks cannot be unblocked: {}".format(self._blocked))
        return results

    def build_async(self, *args, **kwargs):
        '''
        Asyncronously build all tasks in tree.

        Functions recive parameters as:
        <function>(<taskId>, return_values[<requires>], *args, **kwargs)
        Optional args and kwargs passed to task function.
        '''
        self._check_undefined()
        pool = multiprocessing.Pool(**self.poolargs)
        completedQueue = multiprocessing.Manager().Queue()
        asyncResults = {}
        results = {}
        while len(self):
            if len(self._unblocked):
                taskId = self._pop()
                reqres = [results[t] for t in self._taskRequirements[taskId]]
                asyncResults[taskId] = pool.apply_async(
                    _executer, (completedQueue,
                        self._taskFunction[taskId],
                        taskId,
                        reqres,
                        args,
                        kwargs),
                    error_callback=_on_error)
            elif len(self._inprocess):
                try:
                    taskId = completedQueue.get(timeout=self.timeout)
                except:
                    raise Exception("Tasks timed out: {}".format(
                        self._inprocess))
                results[taskId] = asyncResults[taskId].get()
                self._complete(taskId)
            else:
                raise Exception("Tasks cannot be unblocked: {}".format(
                    self._blocked))
        pool.close()
        return results

    def get_undefined_tasks(self):
        '''return required taskIds that are undefined'''
        undefined = []
        for b in self._taskBlocks.keys():
            if b not in self._taskFunction:
                undefined.append(b)
        return undefined

    def get_requirements(self):
        '''return requirements by taskId'''
        return self._taskRequirements

    def get_blocked_tasks(self):
        '''return blocked tasks by taskId'''
        return self._taskBlocks

    def reset(self):
        '''Reset task tree'''
        self._blocked = []
        self._unblocked = []
        self._completed = []
        self._inprocess = []
        for taskId, requires in self._taskRequirements.items():
            self._taskBlockedby[taskId] = requires[:]
            if len(requires):
                self._blocked.append(taskId)
            else:
                self._unblocked.append(taskId)

    def _pop(self):
        taskId = self._unblocked.pop()
        self._inprocess.append(taskId)
        logging.debug('Queueing {} (Queue length: {})'.format(
            taskId, len(self._inprocess)))
        return taskId

    def _complete(self, taskId):
        '''unblock dependent tasks'''
        self._inprocess.remove(taskId)
        if taskId in self._taskBlocks:
            for b in self._taskBlocks[taskId]:
                self._taskBlockedby[b].remove(taskId)
                if len(self._taskBlockedby[b]) < 1:
                    self._blocked.remove(b)
                    self._unblocked.append(b)
        self._completed.append(taskId)
        logging.debug('Completed {} of {}'.format(
            len(self._completed), len(self) + len(self._completed)))

    def _check_undefined(self):
        undefined = self.get_undefined_tasks()
        if len(undefined):
            raise Exception('Required tasks not defined: {}'.format(undefined))

def _executer(queue, func, taskId, requires, args, kwargs):
    r = func(taskId, requires, *args, **kwargs)
    queue.put(taskId)
    return r

def _on_error(e):
    raise e

def test():
    import time
    tree = TaskTree()

    def get(taskId, requires):
        time.sleep(.5)
        print ('Got {}'.format(taskId))
        return taskId

    def make(taskId, requires):
        time.sleep(1)
        print ('Made {} from {}'.format(taskId, ', '.join(requires)))
        return "homemade " + taskId

    tree.add(make, 'pb&j', ['peanutbutter', 'jelly', 'bread'])
    tree.add(make, 'peanutbutter', ['peanuts', 'salt'])
    tree.add(make, 'jelly', ['berries', 'sugar', 'pectin', 'water'])
    tree.add(make, 'bread', ['flour', 'yeast', 'salt', 'water'])
    tree.add(make, 'flour', ['wheat'])

    # we still need to define tasks for the rest of the raw ingredients
    for r in tree.get_undefined_tasks():
        # we'll just assume we can get
        tree.add(get, r)

    tree.build()
    tree.reset()
    tree.build_async()

if __name__ == "__main__":
    test()
