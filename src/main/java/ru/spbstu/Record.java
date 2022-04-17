package ru.spbstu;

import java.nio.ByteBuffer;

public class Record implements Comparable<Record>  {

    private final ByteBuffer key;
    private final ByteBuffer value;

    public Record(ByteBuffer key,
                  ByteBuffer value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(Record o) {
        return 0;
    }
}
