__author__ = 'CJ'
import mesos.interface
from mesos.interface import mesos_pb2
from jobTree.batchSystems.abstractBatchSystem import AbstractBatchSystem
from Queue import Queue

class JobTreeJob:
    def __init__(self, jobID, cpu, memory, command):
        self.jobID = jobID
        self.cpu = cpu
        self.memory = memory
        self.command = command


class ResourceSummary:
    def __init__(self, memory, cpu):
        self.memory = memory
        self.cpu = cpu


class MesosBatchSystem(AbstractBatchSystem):
    def __init__(self, config, maxCpus, maxMemory):
        AbstractBatchSystem.__init__(self, config, maxCpus, maxMemory) #Call the parent constructor
        self.job_type_queue = {}
        self.currentjobs = 0

    def issueJob(self, command, memory, cpu):
        self.checkResourceRequest(memory, cpu)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.currentjobs.add(jobID)

        job = JobTreeJob(jobID=jobID, cpu=cpu, memory=memory, command=command)
        job_type = ResourceSummary(memory=memory, cpu=cpu)

        if job_type in self.job_type_queue:
            self.job_type_queue[job_type].put(job)
        else:
            self.job_type_queue[job_type] = Queue()
            self.job_type_queue[job_type].put(job)

        logger.debug("Issued the job command: %s with job id: %s " % (command, str(jobID)))
        return jobID

    def getJobs(self):
        return self.job_type_queue


class MesosScheduler(mesos.interface.Scheduler):
    def __init__(self, implicitAcknowledgements, executor, batchSystem):
            self.batchSystem = batchSystem
            self.implicitAcknowledgements = implicitAcknowledgements
            self.executor = executor
            self.taskData = {}
            self.tasksLaunched = 0
            self.tasksFinished = 0
            self.messagesSent = 0
            self.messagesReceived = 0

    def registered(self, driver, frameworkId, masterInfo):
        print "Registered with framework ID %s" % frameworkId.value

    def resourceOffers(self, driver, offers):
        # get queues from batch system
        job_queues = self.batchSystem.getJobs

        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            print "Received offer %s with cpus: %s and mem: %s" \
                  % (offer.id.value, offerCpus, offerMem)

            remainingCpus = offerCpus
            remainingMem = offerMem

            job_types = list(job_queues.keys())
            # sorts from largest to smallest cpu usage
            job_types.sort(key=lambda Job: Job.cpu)
            job_types.reverse()

            for job_type in job_types:
                # loop through the resource requirements for queues.
                # if the requirement matches the offer, loop through the queue and
                # assign jobTree jobs as tasks until the offer is used up or the queue empties.

                while (not job_queues[job_type].empty()) and \
                                remainingCpus >= job_type.cpu and \
                                remainingMem >= job_type.memory:

                    jt_job = job_queues[job_type].get()

                    tid = self.tasksLaunched
                    self.tasksLaunched += 1

                    print "Launching task %d using offer %s" \
                          % (tid, offer.id.value)

                    task = mesos_pb2.TaskInfo()
                    task.cpu = job_type.cpu
                    task.memory = job_type.memory

                    task.task_id.value = str(tid)
                    task.slave_id.value = offer.slave_id.value
                    task.name = "task %d" % tid

                    # assigns jobTree command to task
                    task.command.value = jt_job.command

                    task.executor.MergeFrom(self.executor)

                    cpus = task.resources.add()
                    cpus.name = "cpus"
                    cpus.type = mesos_pb2.Value.SCALAR
                    cpus.scalar.value = task.cpu

                    mem = task.resources.add()
                    mem.name = "mem"
                    mem.type = mesos_pb2.Value.SCALAR
                    mem.scalar.value = task.memory

                    tasks.append(task)
                    self.taskData[task.task_id.value] = (
                        offer.slave_id, task.executor.executor_id)

                    remainingCpus -= task.cpu
                    remainingMem -= task.memory

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

    def

    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % \
            (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        # Ensure the binary data came through.
        if update.data != "data with a \0 byte":
            print "The update data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(update.data))
            sys.exit(1)

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasksFinished += 1
            # problem: queues are approximate. Just make this queue.empty()?
            if self.tasksFinished == TOTAL_TASKS:
                print "All tasks done, waiting for final framework message"

            slave_id, executor_id = self.taskData[update.task_id.value]

            self.messagesSent += 1
            driver.sendFrameworkMessage(
                executor_id,
                slave_id,
                'data with a \0 byte')

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            print "Aborting because task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
            driver.abort()

        # Explicitly acknowledge the update if implicit acknowledgements
        # are not being used.
        if not self.implicitAcknowledgements:
            driver.acknowledgeStatusUpdate(update)

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.messagesReceived += 1

        # The message bounced back as expected.
        if message != "data with a \0 byte":
            print "The returned message data did not match!"
            print "  Expected: 'data with a \\x00 byte'"
            print "  Actual:  ", repr(str(message))
            sys.exit(1)
        print "Received message:", repr(str(message))

        if self.messagesReceived == TOTAL_TASKS:
            if self.messagesReceived != self.messagesSent:
                print "Sent", self.messagesSent,
                print "but received", self.messagesReceived
                sys.exit(1)
            print "All tasks done, and all messages received, exiting"
            driver.stop()