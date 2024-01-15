package com.simpledb.utils;

import com.simpledb.transaction.TransactionId;

import java.util.*;

public class WaitForGraph {

    Map<TransactionId, List<TransactionId>> adjList;

    public WaitForGraph() {
        adjList = new HashMap<>();
    }

    public void addVertex(TransactionId vertex) {
        adjList.putIfAbsent(vertex, new LinkedList<>());
    }

    public void removeVertex(TransactionId vertex) {
        adjList.remove(vertex);
    }

    public void addEdge(TransactionId from, TransactionId to) {
        adjList.get(from).add(to);
    }

    public void removeEdge(TransactionId from, TransactionId to) {
        adjList.get(from).remove(to);
    }

    public void removeToEdge(TransactionId from) {
        adjList.put(from, new LinkedList<>());
    }

    public boolean cycleDetect(TransactionId vertex) {
        // vertex does not exits
        if (adjList.getOrDefault(vertex, null) == null) return false;
        List<TransactionId> result = new ArrayList<>();
        result.add(vertex);
        return dfs(vertex, result);
    }

    private boolean dfs(TransactionId vertex, List<TransactionId> result) {
        List<TransactionId> adj = adjList.get(vertex);
        for (TransactionId tid : adj) {
            if (result.contains(tid)) return true;
            result.add(tid);
            return dfs(tid, result);
        }
        return false;
    }

}
