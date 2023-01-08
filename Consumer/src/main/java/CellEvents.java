public class CellEvents {
    public String cellID;
    public EventType eventType;

    public CellEvents(String cellID, EventType eventType) {
        this.cellID = cellID;
        this.eventType = eventType;
    }

    public CellEvents(){}

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getCellID() {
        return cellID;
    }
    public void setCellID(String cellID) {
        this.cellID = cellID;
    }
}

