package com.dd.edata.db.model;


import com.dd.edata.EData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseEntity implements IEntity {
    private static final Logger logger = LoggerFactory.getLogger(BaseEntity.class);
    protected final EData edata;

    protected IModel model;

    public BaseEntity(IModel model, EData edata) {
        this.model = model;
        this.edata = edata;
    }

    @Override
    public IModel getModel() {
        return model;
    }

    @Override
    public void setModel(IModel model) {
        this.model = model;
    }

    @Override
    public boolean insert(boolean async) {
        if (async) {
            edata.insertAsync(model);
            return true;
        }
        try {
            edata.insert(model);
            return true;
        } catch (Exception e) {
            logger.error("insert {} error!!", this, e);
        }
        return false;
    }

    @Override
    public boolean update(boolean async) {
        if (async) {
            edata.updateAsync(model);
            return true;
        }
        try {
            return edata.update(model) > 0;
        } catch (Exception e) {
            logger.error("update {} error!!", this, e);
        }
        return false;
    }

    @Override
    public boolean delete(boolean async) {
        if (async) {
            edata.deleteAsync(model);
            return true;
        }
        try {
            edata.delete(model);
            return true;
        } catch (Exception e) {
            logger.error("delete {} error!!", this, e);
        }
        return false;
    }
}
