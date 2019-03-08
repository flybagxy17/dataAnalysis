import java.util.ArrayList;

public class User {
    private ArrayList json;
    private String code;
    private String message;

    public User() {
    }

    public User(ArrayList json, String code, String message) {
        this.json = json;
        this.code = code;
        this.message = message;
    }

    public ArrayList getJson() {
        return json;
    }

    public void setJson(ArrayList json) {
        this.json = json;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
