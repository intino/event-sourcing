package systems.intino.test;

public class Messages {

    public static String MessageWithParentClass =
        "[Teacher]\n" +
        "name: Jose\n" +
        "money: 50.0\n" +
        "birthDate: 2016-10-04T20:10:12Z\n" +
        "university: ULPGC\n" +
        "\n" +
        "[Teacher.Country]\n" +
        "name: Spain\n";


    public static String EmptyAttributeMessage =
        "[Teacher]\n" +
        "name: Jose\n" +
        "money: 50.0\n" +
        "birthDate: 2016-10-04T20:10:11Z\n" +
        "university: ULPGC\n" +
        "\n" +
        "[Person.Country]\n" +
        "name: Spain\n" +
        "continent:\n";


    static String Status2 =
            "[Status]\n" +
                    "battery: 78.0\n" +
                    "cpuUsage: 11.95\n" +
                    "isPlugged: true\n" +
                    "isScreenOn: true\n" +
                    "temperature: 29.0\n" +
                    "created: 2017-03-22T12:56:18Z\n";


    public final static String StatusMessage =
        ("[Status]\n" +
                "battery: 78.0\n" +
                "cpuUsage: 11.95\n" +
                "isPlugged: true\n" +
                "isScreenOn: false\n" +
                "temperature: 29.0\n" +
                "created: 2017-03-22T12:56:18Z\n") + "\n" + Status2;

    private static String indent(String text) {
        return "\t" + text.replaceAll("\\n", "\n\t");
    }

}
