
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

import java.nio.charset.Charset

object Blowfish {

	val ZampBFKey = "zamplus.com"

	val cipher = new ThreadLocal[Cipher]() {
		override def initialValue() = {
			val cipher = Cipher.getInstance("Blowfish");
			val keySpec = new SecretKeySpec(ZampBFKey.getBytes(Charset.forName("UTF-8")), "Blowfish");
			cipher.init(Cipher.DECRYPT_MODE, keySpec);
			cipher
		}
	}

	def decode(data: Array[Byte]) = synchronized {
		cipher.get.doFinal(data)
	}
}