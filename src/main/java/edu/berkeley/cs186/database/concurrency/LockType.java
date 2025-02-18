package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.common.Pair;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    private static boolean compareLocks(Set<Pair<LockType, LockType>> validPairs, LockType a, LockType b) {
        Pair<LockType, LockType> pair1 = new Pair<>(a,b);
        Pair<LockType, LockType> pair2 = new Pair<>(b,a);
        return validPairs.contains(pair1) || validPairs.contains(pair2);
    }

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        if(a == LockType.NL || b == LockType.NL) {
            return true;
        }
        if(a == LockType.X || b == LockType.X) {
            return false;
        }
        Set<Pair<LockType, LockType>> validPairs = new HashSet<>();
        validPairs.add(new Pair(LockType.IS, LockType.IS));
        validPairs.add(new Pair(LockType.IS, LockType.IX));
        validPairs.add(new Pair(LockType.IS, LockType.S));
        validPairs.add(new Pair(LockType.IS, LockType.SIX));
        validPairs.add(new Pair(LockType.IX, LockType.IX));
        validPairs.add(new Pair(LockType.S, LockType.S));
        // TODO(proj4_part1): implement

        return compareLocks(validPairs, a, b);
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        if (parentLockType == LockType.IX || childLockType == LockType.NL) {
            return true;
        }
        // TODO(proj4_part1): implement
//        System.out.println(parentLock(childLockType));
//        System.out.println(parentLockType);
//        System.out.println(parentLock(childLockType) == parentLockType);

        return parentLock(childLockType) == parentLockType;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        if(substitute == required) {
            return  true;
        }
        if(substitute == LockType.IS) {
            return false;
        }
        if(required == LockType.IS) {
            return substitute == LockType.S || substitute == LockType.X || substitute == LockType.SIX || substitute == LockType.IX;
        }
        if(required == LockType.S || required == LockType.IX) {
            return substitute == LockType.X || substitute == LockType.SIX;
        }
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

