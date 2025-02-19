package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // Do nothing if the transaction or lockContext is null, or if NL requested
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null || requestType == LockType.NL) {
            return;
        }

        // 1) Fix ancestor locks so they hold the correct "intention" locks.
        //    - If requesting S at the child, each ancestor must hold at least IS.
        //    - If requesting X at the child, each ancestor must hold at least IX.
        fixAncestorLocks(lockContext, requestType, transaction);

        // 2) Now ensure the current context holds or promotes to a lock that covers `requestType`.
        LockType effectiveType = lockContext.getEffectiveLockType(transaction);
        LockType explicitType = lockContext.getExplicitLockType(transaction);

        // If the effective lock (including ancestor coverage) already substitutes the requested type,
        // we're done, except for a special "IX vs. S" case: IX alone doesn't grant read permission.
        if (LockType.substitutable(effectiveType, requestType)) {
            // Special case: if effective is IX but we actually need S, we must promote to SIX.
            if (effectiveType == LockType.IX && requestType == LockType.S) {
                lockContext.promote(transaction, LockType.SIX);
            }
            return;
        }

        // Otherwise, the explicit lock here is insufficient. Acquire or promote.
        if (explicitType == LockType.NL) {
            // No lock => acquire exactly what's needed.
            lockContext.acquire(transaction, requestType);
        } else {
            // We hold some explicit lock (IS or IX, etc.) => promote.
            if (requestType == LockType.S) {
                // If we currently hold IS, promote to S; if we hold IX, promote to SIX.
                if (explicitType == LockType.IS) {
                    lockContext.promote(transaction, LockType.S);
                } else if (explicitType == LockType.IX) {
                    lockContext.promote(transaction, LockType.SIX);
                }
            } else {
                // requestType = X => always promote to X (from IS/IX/S, etc.)
                lockContext.promote(transaction, LockType.X);
            }
        }
    }

    /**
     * Ensures that all ancestors of `lockContext` hold at least IS if `requestType == S`,
     * or at least IX if `requestType == X`. (No changes for NL.)
     *
     * We do this from the top (root) down to the direct parent of `lockContext`,
     * so that each parent is guaranteed to hold a large-enough intention lock
     * before we fix its child.
     */
    private static void fixAncestorLocks(LockContext lockContext,
                                         LockType requestType,
                                         TransactionContext transaction) {
        // Build the chain of ancestors from root down to (but not including) this lockContext.
        Deque<LockContext> ancestors = new ArrayDeque<>();
        LockContext cur = lockContext.parentContext();
        while (cur != null) {
            ancestors.push(cur);
            cur = cur.parentContext();
        }

        // Determine which intention lock ancestors need
        // (S => IS, X => IX).
        LockType neededIntent = (requestType == LockType.S) ? LockType.IS : LockType.IX;

        // Process ancestors from top to bottom
        while (!ancestors.isEmpty()) {
            LockContext ancestor = ancestors.pop();
            LockType explicit = ancestor.getExplicitLockType(transaction);
            LockType effective = ancestor.getEffectiveLockType(transaction);

            // If the ancestor’s effective lock already substitutes for neededIntent, do nothing.
            // (For example, if the ancestor is X, or SIX, or IX and we only need IS).
            // But if it does NOT, we either acquire or promote an explicit lock to at least neededIntent.
            if (!LockType.substitutable(effective, neededIntent)) {
                if (explicit == LockType.NL) {
                    // No explicit lock => acquire needed intention
                    ancestor.acquire(transaction, neededIntent);
                } else {
                    // Already hold something, but not enough => promote
                    // e.g. from IS -> IX, or from NL -> IX in a weird leftover scenario, etc.
                    if (neededIntent == LockType.IS) {
                        // We want IS, but the ancestor might hold something else like IX or NL
                        // Normally IX is actually "stronger" than IS for write intent,
                        // but the system might not treat it as substitutable for read. So let's see:
                        // If explicit is IX, that is substitutable for IS. => No action needed
                        // But if explicit is S or X or SIX, it’s also substitutable => no action needed
                        // If explicit is something weaker or incomparable, we promote to IS.
                        if (!LockType.substitutable(explicit, LockType.IS)) {
                            ancestor.promote(transaction, LockType.IS);
                        }
                    } else { // neededIntent == IX
                        // If the ancestor has X or SIX, that’s also substitutable => no action
                        // If the ancestor has IS or S, we promote to IX
                        if (!LockType.substitutable(explicit, LockType.IX)) {
                            ancestor.promote(transaction, LockType.IX);
                        }
                    }
                }
            }
        }
    }
}
