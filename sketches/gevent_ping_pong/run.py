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
from scipy.stats import beta, uniform
from collections import Counter
import gevent
from gevent.queue import Queue
from gevent import Greenlet
from copy import deepcopy
from matplotlib.pyplot import savefig
from matplotlib.pylab import plt

TRUE_CTR = [.02, .012, .03]
# initialize priors as if we'd seen 3 clicks and 100 impressions
CTR_DISTS = [beta(3, 97)] * len(TRUE_CTR)
UNIFORM = uniform(0, 1)

OUTCOMES = Counter()
# initialize OUTCOMES to reflect prior ^^
for ix, d in enumerate(CTR_DISTS):
    OUTCOMES.update({(ix, b): v for b, v in zip((True, False), d.args)})

CTA_IDX_STACK = range(len(TRUE_CTR))


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

    def receive(self, message):
        cta_idx = self.stack.pop()
        outcome = is_clicked(cta_idx)
        outcome_event = (cta_idx, True if outcome else False)
        OUTCOMES[outcome_event] += 1
        if float(UNIFORM.rvs(1)) < (1 / 300.):
            print "goin' for an update!"
            update.inbox.put('stop')
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
        self.target_size = 20000
        self.history = []

    def receive(self, message):
        print "updating: %s" % self.count
        print "message: %s" % message
        self.count += 1
        self.update_task()
        view.inbox.put('go')
        gevent.sleep(0)

    def update_task(self):
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

        # totally arbitrary "capital requirements":
        # if you're replacing 1/6 of your resevoir,
        # increase the resevoir size by half batch size
        if self.last_batch > (self.target_size / 3.):
            self.target_size += (self.last_batch / 2)

        to_add = self.target_size - len(self.stack)
        self.last_batch = to_add
        vals = np.vstack([dist.rvs(to_add) for dist in self.dists])
        self.stack += [vals[:, i].argmax() for i in range(len(vals[0, :]))]
        print "At end of update, stack len is %s " % len(self.stack)

        self.history.append((
            deepcopy(OUTCOMES),
            self.last_batch,
            self.target_size)
        )
        plot_ctr_dists("dists.png")


def plot_ctr_dists(path):
    for dist in CTR_DISTS:
        sns.kdeplot(dist.rvs(500), bw=.001)
    for truth in TRUE_CTR:
        plt.axvline(x=truth)
    savefig(path, bbox_inches='tight')
    plt.clf()
    plt.cla()


if __name__ == '__main__':
    view = ImpressionViewer(CTA_IDX_STACK)
    update = UpdatePusher(CTA_IDX_STACK, CTR_DISTS)

    view.start()
    update.start()

    view.inbox.put(2)
    update.inbox.put('setup')

    gevent.joinall([view, update])
