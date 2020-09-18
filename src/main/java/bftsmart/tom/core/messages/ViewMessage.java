package bftsmart.tom.core.messages;

import bftsmart.communication.SystemMessage;
import bftsmart.reconfiguration.views.View;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ViewMessage extends SystemMessage {

    private View view;

    public ViewMessage() {
    }

    /**
     * Constructor
     * @param from replica that creates this message
     * @param view type of the message (STOP, SYNC, CATCH-UP)
     */
    public ViewMessage(int from, View view) {
        super(from);
        this.view = view;
    }

    public View getView() {
        return view;
    }

    public void setView(View view) {
        this.view = view;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(view);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        view = (View)in.readObject();
    }
}
