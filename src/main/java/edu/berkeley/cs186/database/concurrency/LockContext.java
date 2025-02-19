package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (this.readonly) {
            throw new UnsupportedOperationException("Read only context");
        }
        if(lockType == LockType.NL) {
            throw new InvalidLockException("Lock (NL).");
        }
        // If there's a parent, check that its lock type permits acquiring lockType on this context.
        if (this.parent != null) {
            LockType parentLockType = lockman.getLockType(transaction, this.parent.name);
            // Use switch-case to enforce multigranularity constraints.
            switch (parentLockType) {
                case NL:
                    // No lock held on parent is not acceptable.
                    throw new InvalidLockException("Parent context must have an intention lock (not NL).");

                case IS:
                    // With parent's IS, acquiring a lock that requires write intent (IX or X) is invalid.
                    if (lockType == LockType.IX || lockType == LockType.X) {
                        throw new InvalidLockException("Parent's IS lock does not support acquiring " + lockType + " on a descendant.");
                    }
                    break;

                case IX:
                    // Parent's IX lock is strong enough to allow descendant locks of any kind.
                    break;

                case S:
                    // A shared lock on the parent is not sufficient for acquiring any descendant lock.
                    throw new InvalidLockException("Parent's S lock does not support acquiring descendant locks.");

                case SIX:
                    // If parent holds SIX, acquiring an IS or S is redundant.
                    if (lockType == LockType.IS || lockType == LockType.S) {
                        throw new InvalidLockException("Acquiring " + lockType + " is redundant when parent's lock is SIX.");
                    }
                    break;

                case X:
                    // Exclusive lock on the parent already covers the entire subtree.
                    throw new InvalidLockException("Acquiring a lock on a descendant is redundant when parent's lock is X.");

                default:
                    throw new InvalidLockException("Unknown parent's lock type: " + parentLockType);
            }
        }

        // If we pass the parent's lock check, proceed to acquire the lock.
        lockman.acquire(transaction, name, lockType);

        // Update parent's child lock count so that LockContext#getNumChildren works properly.
        if (this.parent != null) {
            long transNum = transaction.getTransNum();
            // Assume parent's childLockCount is declared as: Map<Long, Integer> childLockCount;
            int childCount = this.parent.numChildLocks.getOrDefault(transNum, 0);
            this.parent.numChildLocks.put(transNum, childCount + 1);
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        if (this.readonly) {
            throw new UnsupportedOperationException("Read only context");
        }

        // Check that the transaction holds a lock on this resource.
        LockType currentLock = lockman.getLockType(transaction, this.name);
        if (currentLock == LockType.NL) {
            throw new NoLockHeldException("Transaction holds no lock on resource " + this.name);
        }

        // Check multigranularity constraints:
        // If the transaction holds any locks in descendant contexts of this resource,
        // releasing the lock here would violate the invariant.
        int descendantCount = this.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
        if (descendantCount > 0) {
            throw new InvalidLockException("Cannot release lock on " + this.name +
                    " because descendant locks exist.");
        }

        // Release the lock on this resource with a single mutating call.
        lockman.release(transaction, this.name);

        // Update parent's child lock count, if a parent exists.
        if (this.parent != null) {
            long transNum = transaction.getTransNum();
            int parentCount = this.parent.numChildLocks.getOrDefault(transNum, 0);
            if (parentCount > 0) {
                this.parent.numChildLocks.put(transNum, parentCount - 1);
            }
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (this.readonly) {
            throw new UnsupportedOperationException("Read-only context");
        }

        // 1. Retrieve the lock held by the transaction on this context.
        LockType currentLock = lockman.getLockType(transaction, this.name);
        if (currentLock == LockType.NL) {
            throw new NoLockHeldException("Transaction holds no lock on resource " + this.name);
        }

        // 2. If the transaction already holds the new lock type, throw an exception.
        if (currentLock == newLockType) {
            throw new DuplicateLockRequestException("Transaction already holds a " + newLockType + " lock on " + this.name);
        }

        // 3. If there's a parent, ensure that the parent's lock type permits having a descendant with the new lock type.
        if (this.parent != null) {
            LockType parentLockType = lockman.getLockType(transaction, this.parent.name);
            // For an X promotion, parent's lock must be at least IX, SIX, or X.
            if (newLockType == LockType.X && (parentLockType == LockType.IS || parentLockType == LockType.S)) {
                throw new InvalidLockException("Parent's lock " + parentLockType + " is insufficient for promotion to X on a descendant.");
            }
            // For SIX promotion, parent's lock must also be strong enough.
            if (newLockType == LockType.SIX && (parentLockType == LockType.IS || parentLockType == LockType.S)) {
                throw new InvalidLockException("Parent's lock " + parentLockType + " is insufficient for promotion to SIX on a descendant.");
            }
            // Also, if an ancestor already holds SIX, promoting to SIX is invalid.
            if (newLockType == LockType.SIX && hasSIXAncestor(transaction)) {
                throw new InvalidLockException("Cannot promote to SIX because an ancestor already holds a SIX lock.");
            }
        }

        // 4. Check that the promotion is valid.
        // Either the new lock is substitutable for the current lock (and not equal), or it's a SIX promotion
        // from one of IS, IX, or S.
        boolean validPromotion = (LockType.substitutable(newLockType, currentLock) && newLockType != currentLock)
                || (newLockType == LockType.SIX &&
                (currentLock == LockType.IS || currentLock == LockType.IX || currentLock == LockType.S));
        if (!validPromotion) {
            throw new InvalidLockException("Invalid promotion from " + currentLock + " to " + newLockType);
        }

        // 5. For a SIX promotion, we need to release all descendant S/IS locks.
        List<ResourceName> releaseLocks = new ArrayList<>();
        if (newLockType == LockType.SIX) {
            releaseLocks = sisDescendants(transaction);
        }
        if (currentLock != LockType.NL) {
            releaseLocks.add(name);
        }
        // 6. Perform the promotion atomically: acquire the new lock on this context and release the designated descendant locks.
        lockman.acquireAndRelease(transaction, this.name, newLockType, releaseLocks);

        // 7. Update this context's child lock count for the transaction since descendant S/IS locks have been released.
        this.numChildLocks.put(transaction.getTransNum(), 0);
        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (this.readonly) {
            throw new UnsupportedOperationException("Read-only context");
        }

        // Get the lock held on THIS context for this transaction.
        // (Assuming lockman.getLock(transaction, resourceName) returns the Lock held on that resource.)
        LockType currentLock = lockman.getLockType(transaction, this.name);
        if (currentLock == LockType.NL) {
            throw new NoLockHeldException("Transaction holds no lock on " + this.name);
        }

        // Get all locks held by the transaction.
        List<Lock> transactionLocks = lockman.getLocks(transaction);

        // List to accumulate descendant resource names whose locks need to be released.
        List<ResourceName> descendants = new ArrayList<>();

        // Determine the escalated lock type.
        // Default to S; if any lock (on this context or a descendant) is more than read-only (i.e. X, IX, or SIX),
        // then we must escalate to X.
        LockType escalatedType = LockType.S;

        for (Lock lock : transactionLocks) {
            // Check locks that are held on THIS context or on its descendants.
            if (lock.name.equals(this.name) || lock.name.isDescendantOf(this.name)) {
                // If the lock type is not S or IS, then we need an X lock.
                if (lock.lockType != LockType.S && lock.lockType != LockType.IS) {
                    escalatedType = LockType.X;
                }
                // If the lock is not on this context, then it is a descendant lock to be released.
                if (!lock.name.equals(this.name)) {
                    descendants.add(lock.name);
                }
            }
        }

        // If there are no descendant locks to release and the current lock is already the proper type,
        // no escalation is necessary.
        if (descendants.isEmpty() && currentLock == escalatedType) {
            return;
        }
        descendants.add(name);
        // Perform the escalation in one atomic call:
        // Acquire the new escalated lock on this context and release all descendant locks.
        lockman.acquireAndRelease(transaction, this.name, escalatedType, descendants);

        // Update this context's child lock count for the transaction to 0,
        // since all descendant locks are being removed.
        this.numChildLocks.put(transaction.getTransNum(), 0);
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        return lockman.getLockType(transaction, this.name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        // If there's no transaction, no lock is effectively held.
        if (transaction == null) {
            return LockType.NL;
        }

        // 1. Check if there's an *explicit* lock at this context.
        //    If yes, that’s the effective lock type.
        LockType explicit = getExplicitLockType(transaction);
        if (explicit != LockType.NL) {
            return explicit;
        }

        // 2. Otherwise, we climb up the parent chain, looking for an explicit
        //    S, X, or SIX. If we find SIX, that confers S at this level.
        LockContext current = this.parent;
        while (current != null) {
            LockType parentExplicit = current.getExplicitLockType(transaction);

            if (parentExplicit == LockType.S || parentExplicit == LockType.X) {
                // Found an ancestor with an explicit S or X => that confers S or X here.
                return parentExplicit;
            } else if (parentExplicit == LockType.SIX) {
                // SIX at an ancestor effectively means S at descendants.
                return LockType.S;
            }
            System.out.println(parentExplicit);
            // If the parent has only IS or IX or NL, keep climbing.
            current = current.parent;
        }

        // 3. If we get here, no ancestor had an explicit S, X, or SIX.
        return LockType.NL;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        return new ArrayList<>();
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

