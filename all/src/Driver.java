public class Driver {
	
	public static void main(String[] args) {
		if (args.length == 2)
			System.out.println("need to write program that runs queries in " + args[1] + " on data in " + args[0]);
		else
			System.err.println("Invalid Arguments: " +  Driver.class.getSimpleName() + " [path to data directory] [query file]");
	}

}
