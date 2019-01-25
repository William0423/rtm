
/**
  * Domain parser, it uses tld data for top level domains.
  *  
  * You can refer to http://www.openrfc.org/rfc/1035.txt
  * for more information about format of domain.
  *   
  *     <domain> ::= <subdomain> | " "
  * 
  *     <subdomain> ::= <label> | <subdomain> "." <label>
  * 
  *     <label> ::= <letter> [ [ <ldh-str> ] <let-dig> ]
  * 
  *     <ldh-str> ::= <let-dig-hyp> | <let-dig-hyp> <ldh-str>
  * 
  *     <let-dig-hyp> ::= <let-dig> | "-"
  * 
  *     <let-dig> ::= <letter> | <digit>
  * 
  *     <letter> ::= any one of the 52 alphabetic characters A through Z in
  *     upper case and a through z in lower case
  * 
  *     <digit> ::= any one of the ten digits 0 through 9
  */

class DomainParser(tldString: String) {

    val topLevelDomains = new scala.collection.mutable.HashSet[String]

    loadTldString(tldString)

    def this() { this("") }

    def expandTld(tldString: String, parentDomain: String, start: Int): Int = {
        /**
          * multiple sub domains.
          */
        if (tldString(start) == '(') {
            /**
              * subdomains start with the form "number:"
              */
            val pos = tldString.indexOf(':', start)
            val num = tldString.substring(start+1, pos).toInt
            
            var subStartPos = pos + 1
            for (subnode <- 1 to num) {
                subStartPos = expandTld(tldString, parentDomain, subStartPos) + 1
            }
            subStartPos
        } else {
            /**
              * take next subdomain from tldstring.
              */
            def labelBorder(s: Int): Int = {
                var pos = s;
                while (pos < tldString.length &&
                    tldString(pos) != ',' &&
                    tldString(pos) != '(' &&
                    tldString(pos) != ')') 
                    pos = pos + 1;
                pos
            }

            val endOfLabel = labelBorder(start)
            val label = tldString.substring(start, endOfLabel)
            
            val subdomain = if (parentDomain.nonEmpty) {
                label + "." + parentDomain
            } else {
                label
            }
            val endChar = if (endOfLabel < tldString.length) {
                tldString(endOfLabel)
            } else {
                '\0'
            }

            if (endChar == '(') {
                /**
                  * Subdomain itself has more subdomains.
                  */
                expandTld(tldString, subdomain, endOfLabel)
            } else {
                topLevelDomains += subdomain
                endOfLabel
            }
        }
    }

    def loadTldString(tldString: String) = {
        if (tldString.startsWith("root(")) {
            /**
              * strip leading "root"
              */
            expandTld(tldString, "", 4)
        } else {
            expandTld(tldString, "", 0)
        }
    }

    def loadTldFile(file: String) = {
        loadTldString(scala.io.Source.fromFile(file).mkString)
        this
    }

    def print() {
        topLevelDomains.foreach(println(_))
    }

    def getDomainAndChannel(url: String) = {
        val host = try {
                new java.net.URL(url).getHost().toLowerCase
            } catch {
                case e: java.net.MalformedURLException => ""
            }
        val labels = host.split("\\.")

        def probeDomain(): (String, Int) = {
            for (i <- 1 until labels.size) {
                val d = labels.takeRight(labels.size - i).mkString(".")

                if (topLevelDomains.contains(d)) {
                    return (labels(i-1) + "." + d, i-1)
                }
                if (topLevelDomains.contains("*."+d)) {
                    return (labels(i-1) + "." + d, i-1)
                }
                /**
                  *  For pattern like '!city.kawasaki.jp', only 9 patterns in file, could be ignored.
                  *  if (topLevelDomains.contains("!"+d)) {
                  *     return (d, i)
                  *  }
                  */
            }
            ("N/A", -1)
        }

        val (domain, index) = probeDomain()
        val channel = if (index > 0) {
            labels(index-1) + "." + domain
        } else {
            domain
        }

        (domain, channel)
    }
}

object DomainParser extends DomainParser(TldString.value)