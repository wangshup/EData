package com.dd.edata.db.model;

public interface IEntity {

    IModel getModel();

    void setModel(IModel model);

    boolean insert(boolean async);

    boolean update(boolean async);

    boolean delete(boolean async);
}
