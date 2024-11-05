package data;

public class Payload {
    public String name;
    public String address;
    public String dateOfBirth;

    public String toString(){
        return name + "," + address + "," + dateOfBirth;
    }
}
