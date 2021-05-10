import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Scanner;

public class MapMR extends Hadoop {
    int i;

    MapMR() {
        i = 0;
    }

    Scanner s = new Scanner(System.in);
    String header = "Reg. No\tName\tMail ID\tGender";

    public boolean sectioner(int num) {
        switch (sec) {
            case 'A':
                if (num >= 1 && num <= 60 || num >= 239 && num <= 243) {
                    return true;
                }
                break;
            case 'B':
                if (num >= 61 && num <= 120 || num >= 244 && num <= 247) {
                    return true;
                }
                break;
            case 'C':
                if ((num >= 121 && num <= 179) || (num >= 249 && num <= 254)) {
                    return true;
                }
                break;
            case 'D':
                if (num >= 180 && num <= 238 || num >= 255 && num <= 258) {
                    return true;
                }
                break;
            case 'N':
                return true;
            default:
                System.out.println("\t\t~Section Not Found!~");
                return false;
        }
        return false;
    }

    public void playdb_s1(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        i += 1;
        String[] data;
        for (Text val : values) {
            String temp = val.toString();
            context.write(new Text(String.valueOf(i)), val);
        }
    }

    public void playdb_s2(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data;
        String roll = "", ssc = "";
        int num;
        for (Text val : values) {
            String temp = val.toString();
            data = temp.split("\t");
            roll = data[0];
            num = Integer.parseInt(roll.substring(5, 8));
            ssc = roll.substring(2, 5);
            if (sectioner(num)) {
                i += 1;
                context.write(new Text(String.valueOf(i)), val);
            }
        }
    }

    public void playdb_s3(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        String[] data;
        String gend = "";
        for (Text val : values) {
            String temp = val.toString();
            data = temp.split("\t");
            gend = data[3];
            if (gend.equalsIgnoreCase(gender)) {
                i += 1;
                context.write(new Text(String.valueOf(i)), val);
            }
        }
    }

    public void playdb_s4(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data;
        String name = "";
        for (Text val : values) {
            String temp = val.toString();
            data = temp.split("\t");
            name = data[1];
            if (name.startsWith(String.valueOf(alp))) {
                i += 1;
                context.write(new Text(String.valueOf(i)), val);
            }
        }
    }

    public void bgrp_s1(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data;
        String gend = "", name = "", roll = "", grp = "";
        for (Text val : values) {
            String temp = val.toString();
            data = temp.split("\t");
            roll = data[0];
            name = data[1];
            gend = data[3];
            grp = data[data.length - 1];
            i += 1;
            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + grp + "\t" + name + "\t" + gend));
        }
    }

    public void bgrp_s2(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data;
        String gend = "", name = "", roll = "", grp = "";
        for (Text val : values) {
            String temp = val.toString();
            data = temp.split("\t");
            roll = data[0];
            name = data[1];
            gend = data[3];
            grp = data[data.length - 1];
            if (bgrp.equalsIgnoreCase(grp)) {
                switch (gnd) {
                    case 'M':
                        if (gend.equalsIgnoreCase("male")) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + grp + "\t" + name + "\t" + gend));
                        }
                        break;
                    case 'F':
                        if (gend.equalsIgnoreCase("female")) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + grp + "\t" + name + "\t" + gend));
                        }
                        break;
                    case 'B':
                        context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + grp + "\t" + name + "\t" + gend));
                        break;

                }
            }
        }
    }

    public void mark_s1(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] data;
        int num;
        float tp, twp, ugp;
        String ten = "", name = "", roll = "", twe = "", cgp = "";
        for (Text val : values) {
            String temp = val.toString();
            data = temp.split("\t");
            roll = data[0];
            num = Integer.parseInt(roll.substring(5, 8));
            name = data[1];
            tp = Float.parseFloat(data[4]);
            twp = Float.parseFloat(data[5]);
            ugp = Float.parseFloat(data[6]);
            switch ((ch * 10) + sch) {
                case 31:
                    if (std.equalsIgnoreCase("10th")) {
                        if (tp > mrk && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + tp));
                        }
                    }
                    if (std.equalsIgnoreCase("12th")) {
                        if (twp > mrk && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + twp));
                        }
                    }
                    if (std.equalsIgnoreCase("UG")) {
                        if (ugp > mrk && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + ugp));
                        }
                    }
                    break;
                case 32:
                    if (std.equalsIgnoreCase("10th")) {
                        if (tp < mrk && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + tp));
                        }
                    }
                    if (std.equalsIgnoreCase("12th")) {
                        if (twp < mrk && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + twp));
                        }
                    }
                    if (std.equalsIgnoreCase("UG")) {
                        if (ugp < mrk && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + ugp));
                        }
                    }
                    break;
                case 33:
                    if (std.equalsIgnoreCase("10th")) {
                        if (tp > mrk && tp < mrk1 && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + tp));
                        }
                    }
                    if (std.equalsIgnoreCase("12th")) {
                        if (twp > mrk && twp < mrk1 && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + twp));
                        }
                    }
                    if (std.equalsIgnoreCase("UG")) {
                        if (ugp > mrk && ugp < mrk1 && sectioner(num)) {
                            i += 1;
                            context.write(new Text(String.valueOf(i)), new Text(roll + "\t" + name + "\t" + ugp));
                        }
                    }
                    break;
            }
        }
    }
}