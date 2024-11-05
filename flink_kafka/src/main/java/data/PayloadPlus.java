package data;

public class PayloadPlus {
    public String name;
    public String address;
    public String dateOfBirth;
    public Integer age;

    public String toString(){
        return name + "," + address + "," + dateOfBirth + "," + Integer.toString(age);
    }
}
