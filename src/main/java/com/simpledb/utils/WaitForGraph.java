package com.simpledb.utils;

import com.simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class WaitForGraph {

    Map<TransactionId, List<TransactionId>> adjList;

    public WaitForGraph() {
        adjList = new HashMap<>();
    }

    public void addVertex(TransactionId vertex) {
        adjList.putIfAbsent(vertex, new LinkedList<>());
    }

    public void addEdge(TransactionId from, TransactionId to) {
        adjList.get(from).add(to);
    }

    public void removeEdge(TransactionId from, TransactionId to) {
        adjList.get(from).remove(to);
    }

}
