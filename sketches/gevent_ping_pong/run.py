# -*- coding: utf-8 -*-
"""
    AB Test Ping Pong
    ~~~~~~~~~~~~~~~~~

    Gevent sketch of separate analysis and impression server logic
    Globals used to mock redis datastructures (key-values, stack)

    Impression task caller:
        Receive command:
            Get cta via pop CTA_IDX_STACK
            Get outcome randomly based on cta
            Add outcome to OUTCOMES


    Update task caller:
        Receive command:
            Process new OUTCOMES
            Update CTR_DISTS to reflect new observations
            Push new cta draws to CTA_IDX_STACK



"""
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import beta, uniform, poisson
from collections import Counter, deque
import gevent
from gevent.queue import Queue
from gevent import Greenlet
from copy import deepcopy
from matplotlib.pyplot import savefig
from matplotlib import pylab  # import plt

TRUE_CTR = [.02, .012, .03]
# initialize priors as if we'd seen 3 clicks and 100 impressions
CTR_DISTS = [beta(3, 97)] * len(TRUE_CTR)
UNIFORM = uniform(0, 1)

OUTCOMES = Counter()
# initialize OUTCOMES to reflect prior ^^
for ix, d in enumerate(CTR_DISTS):
    OUTCOMES.update({(ix, b): v for b, v in zip((True, False), d.args)})

# initialize CTA_IDX_STACK
INITIAL_TARGET_SIZE = 1000
CTA_IDX_STACK = deque()
vals = np.vstack([dist.rvs(INITIAL_TARGET_SIZE) for dist in CTR_DISTS])
for i in range(INITIAL_TARGET_SIZE):
    CTA_IDX_STACK.append(vals[:, i].argmax())


class Actor(gevent.Greenlet):
    # per Stephen Diehl
    def __init__(self):
        self.inbox = Queue()
        Greenlet.__init__(self)

    def receive(self, message):
        """
        Define in your subclass.
        """
        raise NotImplemented()

    def _run(self):
        self.running = True

        while self.running:
            message = self.inbox.get()
            self.receive(message)


def is_clicked(cta_idx):
    return UNIFORM.rvs(1) < TRUE_CTR[cta_idx]


class ImpressionViewer(Actor):
    def __init__(self, stack):
        super(ImpressionViewer, self).__init__()
        self.stack = stack
        self.mean_requests_per_batch = 300
        self.batch_index = 0
        self.batch_size_dist = poisson(self.mean_requests_per_batch)
        self.batch_target = self.batch_size_dist.rvs(1)[0]

    def receive(self, message):
        cta_idx = self.stack.pop()
        outcome = is_clicked(cta_idx)
        outcome_event = (cta_idx, True if outcome else False)
        OUTCOMES[outcome_event] += 1
        self.batch_index += 1
        if self.batch_index == self.batch_target:
            print "goin' for an update!"
            update.inbox.put('stop')
            self.batch_target = self.batch_size_dist.rvs(1)[0]
            self.batch_index = 0
            gevent.sleep(0)
        else:
            if len(self.stack) % 1000 == 0:
                print 'stack len: %s ' % len(self.stack)
            view.inbox.put('go')
            gevent.sleep(0)


class UpdatePusher(Actor):
    """
    Pushes selected indices to CTA_IDX_STACK
    """
    def __init__(self, stack, dists):
        super(UpdatePusher, self).__init__()
        self.stack = stack
        self.dists = dists
        self.count = 0
        self.last_batch = 0
        self.target_size = INITIAL_TARGET_SIZE
        self.batch_size = INITIAL_TARGET_SIZE / 4
        self.history = []

    def receive(self, message):
        print "updating: %s" % self.count
        print "message: %s" % message
        self.count += 1
        self.update_task()
        view.inbox.put('go')
        gevent.sleep(0)

    def update_task(self, initial=False):
        """
        Some non-deterministic task
        """
        print "At start of update, stack len is %s " % len(self.stack)
        for idx in range(len(self.dists)):
            new_successes = OUTCOMES[(idx, True)]
            new_failures = OUTCOMES[(idx, False)]
            old_successes, old_failures = self.dists[idx].args
            if not (old_successes, old_failures) == \
                    (new_successes, new_failures):
                self.dists[idx] = beta(
                    new_successes,
                    new_failures)

        # how much was popped since last update?
        just_consumed = self.target_size - len(self.stack)

        # we want batch size to be twice the largest consumption we've seen
        # we could keep an average around and estimate a poisson, but we
        # don't know if the underlying traffic volume distribution is changing
        self.batch_size = max(self.batch_size, (just_consumed * 2))

        # we want an additional reservoir beyond what's pushed each batch
        # as a guard against steeply increasing traffic volume
        self.target_size = max(self.target_size, (self.batch_size * 2))

        # since we intend to push far more entries than we're likely to pop
        # we should remove old entries to keep the stack length stable
        # (if we're pushing to reach a new, larger target size, we may not need this)
        to_trim = max((len(self.stack) + self.batch_size) - self.target_size, 0)

        # draw new entries and push them
        vals = np.vstack([dist.rvs(self.batch_size) for dist in self.dists])
        for i in range(self.batch_size):
            self.stack.append(vals[:, i].argmax())

        # trim entries from the old side
        for i in range(to_trim):
            self.stack.popleft()

        # this is only true in the non-concurrent example
        assert len(self.stack) == self.target_size

        print "At end of update, stack len is %s " % len(self.stack)

        self.history.append((
            self.batch_size,
            self.target_size)
        )
        # plot_ctr_dists("dists.png", index=str(self.count))
        if self.count > 1000:
            df = pd.DataFrame(self.history, columns=['batch_size', 'target_size'])
            df.to_csv('updater_history.csv')
            import ipdb; ipdb.set_trace()  # breakpoint ecd92988 //



def plot_ctr_dists(path, index=''):
    for dist in CTR_DISTS:
        sns.kdeplot(dist.rvs(500), bw=.001)
    for truth in TRUE_CTR:
        pylab.plt.axvline(x=truth)
    pylab.xlim([-.01, .08])
    savefig(path, bbox_inches='tight')
    if index:
        savefig('_'.join([index.rjust(5, '0'), path]), bbox_inches='tight')
    pylab.plt.clf()
    pylab.plt.cla()


if __name__ == '__main__':
    view = ImpressionViewer(CTA_IDX_STACK)
    update = UpdatePusher(CTA_IDX_STACK, CTR_DISTS)

    view.start()
    update.start()

    view.inbox.put(2)
    update.inbox.put('setup')

    gevent.joinall([view, update])
