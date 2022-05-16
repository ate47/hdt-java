/*
 * File: $HeadURL: https://hdt-java.googlecode.com/svn/trunk/hdt-java/iface/org/rdfhdt/hdt/triples/TripleString.java $
 * Revision: $Rev: 191 $
 * Last modified: $Date: 2013-03-03 11:41:43 +0000 (dom, 03 mar 2013) $
 * Last modified by: $Author: mario.arias $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * Contacting the authors:
 *   Mario Arias:               mario.arias@deri.org
 *   Javier D. Fernandez:       jfergar@infor.uva.es
 *   Miguel A. Martinez-Prieto: migumar2@infor.uva.es
 *   Alejandro Andres:          fuzzy.alej@gmail.com
 */

package org.rdfhdt.hdt.triples;

import java.io.IOException;

import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.util.UnicodeEscape;

/**
 * TripleString holds a triple as Strings
 */
public class TripleString {

	private CharSequence subject;
	private CharSequence predicate;
	private CharSequence object;

	public TripleString() {
		this.subject = this.predicate = this.object = null;
	}

	/**
	 * Basic constructor
	 *
	 * @param subject
	 *            The subject
	 * @param predicate
	 *            The predicate
	 * @param object
	 *            The object
	 */
	public TripleString(CharSequence subject, CharSequence predicate, CharSequence object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	/**
	 * Copy constructor
	 * @param other triple string to copy
	 */
	public TripleString(TripleString other) {
		this.subject = other.subject;
		this.predicate = other.predicate;
		this.object = other.object;
	}

	/**
	 * @return CharSequence the subject
	 */
	public CharSequence getSubject() {
		return subject;
	}

	/**
	 * @param subject
	 *            the subject to set
	 */
	public void setSubject(CharSequence subject) {
		this.subject = subject;
	}

	/**
	 * @return CharSequence the predicate
	 */
	public CharSequence getPredicate() {
		return predicate;
	}

	/**
	 * @param predicate
	 *            the predicate to set
	 */
	public void setPredicate(CharSequence predicate) {
		this.predicate = predicate;
	}

	/**
	 * @return the object
	 */
	public CharSequence getObject() {
		return object;
	}

	/**
	 * @param object
	 *            the object to set
	 */
	public void setObject(CharSequence object) {
		this.object = object;
	}

	/**
	 * Sets all components at once. Useful to reuse existing object instead of creating new ones for performance.
	 * @param subject subject
	 * @param predicate predicate
	 * @param object object
	 */
	public void setAll(CharSequence subject, CharSequence predicate, CharSequence object) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	private boolean equalsCharSequence(CharSequence cs1, CharSequence cs2) {
		if (cs1 instanceof String && cs2 instanceof String)
			return cs1.equals(cs2); // use string method if we can

		if (cs1.length() != cs2.length())
			return false;

		for (int i = 0; i < cs1.length(); i++)
			if (cs1.charAt(i) != cs2.charAt(i))
				return false;
		return true;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof TripleString) {
			TripleString ts = (TripleString) other;
			return equalsCharSequence(subject, ts.subject) && equalsCharSequence(predicate, ts.predicate)
					&& equalsCharSequence(object, ts.object);
		}
		return false;
	}

	@Override public int hashCode() {
		// Same as Objects.hashCode(subject, predicate, object), with fewer calls
		int s = subject   == null ? 0 : subject.hashCode();
		int p = predicate == null ? 0 : predicate.hashCode();
		int o = object    == null ? 0 : object.hashCode();
		return 31 * (31 * (31 * s) + p) + o;
	}

	/**
	 * Check whether this triple matches a pattern. A pattern is just a TripleString where each empty component means <em>any</em>.
	 * @param pattern triple pattern to search
	 * @return boolean
	 */
	public boolean match(TripleString pattern) {
        if (pattern.getSubject().length() == 0 || equalsCharSequence(pattern.getSubject(), this.subject)) {
            if (pattern.getPredicate().length() == 0 || equalsCharSequence(pattern.getPredicate(), this.predicate)) {
                if (pattern.getObject().length() == 0 || equalsCharSequence(pattern.getObject(), this.object)) {
                    return true;
                }
            }
        }
        return false;
	}

	/**
	 * Set all components to ""
	 */
	public void clear() {
		subject = predicate = object = "";
	}

	/**
	 * Checks whether all components are empty.
	 * @return boolean
	 */
	public boolean isEmpty() {
		return subject.length()==0 && predicate.length()==0 && object.length()==0;
	}

	/**
	 * Checks whether any component is empty.
	 * @return boolean
	 */
	public boolean hasEmpty() {
		return subject.length()==0 || predicate.length()==0 || object.length()==0;
	}

	/**
	 * Read from a line, where each component is separated by space.
	 * @param line line to read
	 * @throws ParserException if the line is not RDF complient
	 */
	public void read(String line) throws ParserException {
		read(line, 0, line.length());
	}

	private int searchNextTabOrSpace(String line, int start, int end) {
		// searching space
		int sindex = line.indexOf(' ', start);
		if (sindex != -1 && sindex < end) {
			return sindex;
		}

		// not found, searching tabs
		int tindex = line.indexOf('\t', start);
		if (tindex != -1 && tindex < end) {
			return tindex;
		}

		// not found
		return -1;
	}

	/**
	 * Read from a line, where each component is separated by space.
	 * @param line line to read
	 * @throws ParserException if the line is not RDF complient
	 */
	public void read(String line, int start, int end) throws ParserException {
		int split, posa, posb;
		this.clear();

		// SET SUBJECT
		posa = start;
		posb = split = searchNextTabOrSpace(line, posa, end);

		if (posb == -1) {
			// Not found, error.
			return;
		}
		if (line.charAt(posa) == '<') {
			posa++;        // Remove <
			if (line.charAt(posb-1) == '>') {
				posb--;    // Remove >
			}
		}

		this.setSubject(UnicodeEscape.unescapeString(line, posa, posb));

		// SET PREDICATE
		posa = split + 1;
		posb = split = searchNextTabOrSpace(line, posa, end);

		if (posb == -1) {
			return;
		}
		if (line.charAt(posa) == '<') {
			posa++;
			if (posb > posa && line.charAt(posb - 1) == '>') {
				posb--;
			}
		}

		this.setPredicate(UnicodeEscape.unescapeString(line, posa, posb));

		// SET OBJECT
		posa = split + 1;
		posb = end;

		// Remove trailing <space> <dot> from NTRIPLES.
		if (line.charAt(posb-1) == '.') {
			posb--;
		}
		char prev = line.charAt(posb-1);
		if (prev == ' ' || prev == '\t') {
			posb--;
		}

		if (line.charAt(posa) == '<') {
			posa++;

			// Remove trailing > only if < appears, so "some"^^<http://datatype> is kept as-is.
			if (posb > posa && line.charAt(posb-1)=='>') {
				posb--;
			}
		}

		this.setObject(UnicodeEscape.unescapeString(line, posa, posb));
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return subject + " " + predicate + " " + object;
	}

	/**
     * Convert TripleString to NTriple
	 * @return CharSequence
	 * @throws IOException when IOException occurs
	 */
	public CharSequence asNtriple() throws IOException {
		StringBuilder str = new StringBuilder();
		this.dumpNtriple(str);
		return str;
	}

	public final void dumpNtriple(Appendable out) throws IOException {
		char s0 = subject.charAt(0);
		if(s0=='_' || s0=='<') {
			out.append(subject);
		} else {
			out.append('<').append(subject).append('>');
		}

		char p0 = predicate.charAt(0);
		if(p0=='<') {
			out.append(' ').append(predicate).append(' ');
		} else {
			out.append(" <").append(predicate).append("> ");
		}

		char o0 = object.charAt(0);
		if(o0=='"') {
			UnicodeEscape.escapeString(object.toString(), out);
			out.append(" .\n");
		} else if(o0=='_' ||o0=='<' ) {
			out.append(object).append(" .\n");
		} else {
			out.append('<').append(object).append("> .\n");
		}
	}
}
