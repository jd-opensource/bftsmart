/**
 * Copyright: Copyright 2016-2020 JD.COM All Right Reserved
 * FileName: bftsmart.tom.ReplyContext
 * Author: shaozhuguang
 * Department: Y事业部
 * Date: 2018/11/13 下午2:48
 * Description:
 */
package bftsmart.tom;

import bftsmart.tom.core.ReplyManager;
import bftsmart.tom.server.Replier;

/**
 *
 * @author shaozhuguang
 * @create 2018/11/13
 * @since 1.0.0
 */

public class ReplyContext {

    private int id;

    private int currentViewId;

    private int numRepliers;

    private ReplyManager repMan;

    private Replier replier;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getCurrentViewId() {
        return currentViewId;
    }

    public void setCurrentViewId(int currentViewId) {
        this.currentViewId = currentViewId;
    }

    public int getNumRepliers() {
        return numRepliers;
    }

    public void setNumRepliers(int numRepliers) {
        this.numRepliers = numRepliers;
    }

    public ReplyManager getRepMan() {
        return repMan;
    }

    public void setRepMan(ReplyManager repMan) {
        this.repMan = repMan;
    }

    public Replier getReplier() {
        return replier;
    }

    public void setReplier(Replier replier) {
        this.replier = replier;
    }

    public ReplyContext buildId(int id) {
        setId(id);
        return this;
    }

    public ReplyContext buildCurrentViewId(int currentViewId) {
        setCurrentViewId(currentViewId);
        return this;
    }

    public ReplyContext buildNumRepliers(int numRepliers) {
        setNumRepliers(numRepliers);
        return this;
    }

    public ReplyContext buildRepMan(ReplyManager repMan) {
        setRepMan(repMan);
        return this;
    }

    public ReplyContext buildReplier(Replier replier) {
        setReplier(replier);
        return this;
    }

    public static String byte2String(byte[] bytes) {
        String result = null;
        try {
            result = new String(bytes, "UTF-8");
        } catch (Exception e) {
            new RuntimeException(e);
        }
        return result;
    }
}