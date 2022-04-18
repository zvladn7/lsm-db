package ru.spbstu.dao;

public class DeletedValueException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DeletedValueException() {
    }

    public DeletedValueException(final String message) {
        super(message);
    }

}
