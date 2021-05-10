import java.util.Scanner;

public class Choices extends Hadoop {
    int cx;
    Scanner s = new Scanner(System.in);

    public void playdb() {
        System.out.println("\n\t---SUBMENU---\t1)View FullDB\t2)View Section\t3)View Gender\t4)View Names\n");
        System.out.print("Enter Choice : ");
        cx = s.nextInt();
        sch = cx;
        switch (cx) {
            case 1:
                System.out.println("\t\t--Viewing Full DB--\n");
                break;
            case 2:
                System.out.print("Enter Section : 'A/B/C/D' : ");
                sec = s.next().charAt(0);
                System.out.println("\t\t--Viewing '" + sec + "' Section--\n");
                break;
            case 3:
                System.out.print("Enter Gender : 'Male/Female' : ");
                gender = s.next();
                System.out.println("\t\t--Viewing '" + gender + "' Gender--\n");
                break;
            case 4:
                System.out.print("Enter Aplhabet : [A-Z] : ");
                alp = s.next().charAt(0);
                System.out.println("\t\t--Viewing Names from '" + alp + "'--\n");
                break;

        }
    }

    public void bgrp() {
        System.out.println("\n\t---SUBMENU---\t1)View All Groups\t2)View Selection");
        System.out.print("Enter Choice : ");
        cx = s.nextInt();
        sch = cx;
        switch (cx) {
            case 1:
                System.out.println("\t\t--Viewing All Groups--\n");
                break;
            case 2:
                System.out.print("\nEnter Blood Group : ");
                bgrp = s.next();
                System.out.print("\nSelect Gender : 'M/F/B' : ");
                gnd = s.next().charAt(0);
                System.out.println("\t\t--Viewing User Selection--\n");
                System.out.println("\t\t\t--> " + bgrp + " | " + gnd + " <--\n");
                break;
        }
    }

    public void marks() {
        System.out.println("\n\t---SUBMENU---\t1)View Greater Than\t2)View Less Than\t3)View Between");
        System.out.print("Enter Choice : ");
        cx = s.nextInt();
        sch = cx;
        switch (cx) {
            case 1:
                System.out.print("Enter Value : ");
                mrk = s.nextInt();
                System.out.print("\nEnter Category : ");
                std = s.next();
                System.out.print("Enter Section : 'A/B/C/D-N(All)' : ");
                sec = s.next().charAt(0);
                System.out.println("\t\t--Viewing Greater Than--\n");
                System.out.println("\t\t\t--> " + mrk + " | " + sec + " <--\n");
                break;
            case 2:
                System.out.print("Enter Value : ");
                mrk = s.nextInt();
                System.out.print("\nEnter Category : ");
                std = s.next();
                System.out.print("Enter Section : 'A/B/C/D-N(All)' : ");
                sec = s.next().charAt(0);
                System.out.println("\t\t--Viewing Less Than--\n");
                System.out.println("\t\t\t--> " + mrk + " | " + sec + " <--\n");
                break;
            case 3:
                System.out.print("Enter Value1 : ");
                mrk = s.nextInt();
                System.out.print("Enter Value2 : ");
                mrk1 = s.nextInt();
                System.out.print("\nEnter Category : ");
                std = s.next();
                System.out.print("Enter Section : 'A/B/C/D-N(All)' : ");
                sec = s.next().charAt(0);
                System.out.println("\t\t--Viewing Between--\n");
                System.out.println("\t\t\t--> " + mrk + " & " + mrk1 + " | " + sec + " <--\n");
                break;
        }
    }
}