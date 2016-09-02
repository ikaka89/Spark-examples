package org.sparky.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Args {
	
	private static Map<String[], Set<String>> flags = new HashMap<String[],Set<String>>();
    private static Map<String[], Set<String>> arguments = new HashMap<String[],Set<String>>();
	
	/**
	 * Parses all arguments. Similar to parseFlags, but with no restrictions on 
	 * the argument not taking a value <br>
	 * Returns all strings preceded by "<tt>--</tt>"
	 * @param args The command line options of the form <tt>--foo</tt>
	 * @return
	 */
	public static Set<String> parseArgs(String args[]) {
		if (arguments.containsKey(args)) {
			return arguments.get(args);
		}

		HashSet<String> output = new HashSet<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].matches("^--.*")){
				output.add(args[i].substring(2));
			}
		}
		arguments.put(args,output);
		return output;    	
	}

	/** parses flags.  These are command line options of the form 
	 * --foo
	 * followed by another option (eg, --foo --bar quux) or
	 * the end of the command line. They take no value after the name of the argument
	 *
	 * @returns the Set of flags present in args[].  The Strings returned do not include the leading --
	 */
	public static Set<String> parseFlags(String args[]) {
		if (flags.containsKey(args)) {
			return flags.get(args);
		}

		HashSet<String> output = new HashSet<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].matches("^--.*") &&
					((i == args.length - 1) ||
							args[i+1].matches("^--.*"))) {
				output.add(args[i].substring(2));
			}
		}
		flags.put(args,output);
		return output;
	}

	/** Parses the integer value of the argument named by <code>key</code> from the specified command line.   
	 *  If no value is present, returns <code>defaultValue</code>.  If the key is present multiple times 
	 *  on the command line, the first instance is returned.  
	 *  Example:
	 *  parseInteger(args,"foo",10); where args={"--minimum","1.3", "--foo","50"} returns 50.
	 */
	public static int parseInteger(String args[], String key, int defaultValue) {
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				return Integer.parseInt(args[++i]);
			}
		}
		return defaultValue;
	}

	/**
	 * Parses all the integers of the arguments that are named by <tt>key</tt>
	 * and returns them as a <tt>Collection</tt> of <tt>Integers</tt> <br>
	 * Example: parseIntegers(args, "foo"); 
	 * where args ={"--foo", "3", "--min", "2.5", "--foo", "4"}  returns [3, 4].
	 * @param args arguments of the command line
	 * @param key the argument named by <tt>key</tt>
	 * @return
	 */
	public static Collection<Integer> parseIntegers(String args[], String key) {
		ArrayList<Integer> output = new ArrayList<Integer>();
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				output.add(new Integer(args[++i]));
			}
		}
		return output;
	}

	/**
	 * Parses all the doubles of the arguments that are named by <tt>key</tt>
	 * and returns them as a <tt>Collection</tt> of <tt>Doubles</tt> <br>
	 * Example: parseDoubles(args, "foo"); 
	 * where args ={"--foo", "3.2", "--min", "2.5", "--foo", "4.3"}  returns [3.2, 4.3].
	 * @param args arguments of the command line
	 * @param key the argument named by <tt>key</tt>
	 * @return
	 */
	public static Collection<Double> parseDoubles(String args[], String key) {
		ArrayList<Double> output = new ArrayList<Double>();
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				output.add(new Double(args[++i]));
			}
		}
		return output;
	}

	/** Parses a long from the specified command line.
	 * @see org.seqcode.gse.tools.utils.Args.parseInteger
	 */
	public static long parseLong(String args[], String key, long defaultValue) {
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				return Long.parseLong(args[++i]);
			}
		}
		return defaultValue;
	}
	/** Parses a double from the specified command line.
	 * @see org.seqcode.gse.tools.utils.Args.parseInteger
	 */
	public static double parseDouble(String args[], String key, double defaultValue) {
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				return Double.parseDouble(args[++i]);
			}
		}
		return defaultValue;
	}

	/** Parses a float from the specified command line.
	 * @see org.seqcode.gse.tools.utils.Args.parseInteger
	 */
	public static float parseFloat(String args[], String key, float defaultValue) {
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				return Float.parseFloat(args[++i]);
			}
		}
		return defaultValue;
	}
	/** Parses a string from the specified command line.
	 * @see org.seqcode.gse.tools.utils.Args.parseInteger
	 */
	public static String parseString(String args[], String key, String defaultValue) {
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				return args[++i];
			}
		}
		return defaultValue;
	}

	/** Parses all strings of the argument by the name <tt>key<tt>.
	 * @see org.seqcode.gse.tools.utils.Args.parseIntegers
	 */
	public static Collection<String> parseStrings(String args[], String key) {
		ArrayList<String> output = new ArrayList<String>();
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				output.add(args[++i]);
			}
		}
		return output;
	}
	/** Parses a filename from the command line.  The file name is either 
	 * immediately preceded by <tt>--file</tt> or is any argument(s) that come(s)
	 * after <tt>--</tt> and then follows the end of the command line. <br>  
	 * For example:
	 * <tt>--max 10 --min 3 -- foo.txt bar.txt baz.txt</tt>
	 */
	public static List<String> parseFile(String args[]) {
		ArrayList<String> output = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("--file")) {
				output.add(args[++i]);
			}
			if (args[i].equals("--")) {
				for (int j = i + 1; j < args.length; j++) {
					output.add(args[j]);
				}
				break;
			}
		}
		return output;
	}

	/** Parses a list of files of the argument by the name <tt>key</tt> 
	 * from the command line and returns file handles
	 * 
	 */
	public static List<File> parseFileHandles(String args[], String key) {
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		ArrayList<File> output = new ArrayList<File>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				output.add(new File(args[++i]));
			}
		}
		return(output);
	}
	/** This method returns a list containing all values preceded by the specified key.
	 * for example:
	 *   <tt>--input foo.txt --input bar.txt --input baz.txt</tt>
	 * would return the list "foo.txt","bar.txt","baz.txt" if key="input"
	 */
	public static List<String> parseList(String args[], String key) {
		if (!key.matches("^\\-\\-.*")) {
			key = "--" + key;
		}
		ArrayList<String> output = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(key)) {
				output.add(args[++i]);
			}
		}
		return output;
	}

}
