import pymssql
import sys
import ProgramResult
import statistics


class Arac:
    def getFisnoForCustomer(self,customerId):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select DISTINCT sevk.FISNO from SIPARIS sip, SEVK sevk where sevk.SIPID = sip.ID and sip.CARIID = %d', customerId)
        row = cur.fetchone()
        aracID = []
        while row:
            aracID.append(row[0])
            row = cur.fetchone()
        conn.close()
        return aracID

    def aractakiParcaliSiparisSayisi(self,aracNo):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select count(*) from SEVK where FISNO = %d', aracNo)
        row = cur.fetchone()
        parcaliSiparisSayisi = row[0]
        conn.close()
        return parcaliSiparisSayisi

    def musterininAractakiParcaliSiparisSayisi(self,customerId,aracNo):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select count(*)from SIPARIS sip, SEVK sevk where sevk.SIPID = sip.ID and sip.CARIID = %d and sevk.FISNO = %d' % (customerId,aracNo))
        row = cur.fetchone()
        aracID = row[0]
        return aracID

    def percentage(self,part,whole):
        return 100*float(part) / float(whole)

    def musterininParcaliSiparisYuzdesi(self,customerId):
        aracIdleri = self.getFisnoForCustomer(customerId)
        print " %d ID'li musterinin siparislerini tasiyan arac IDleri = %s" %(customerId,aracIdleri)
        yuzdeler = []
        for aracId in aracIdleri:
            parcaliSiparisSayisi = self.aractakiParcaliSiparisSayisi(aracId)
            musteri = self.musterininAractakiParcaliSiparisSayisi(customerId,aracId)
            print "%d nolu aractaki parcali siparis sayisi = %d" %(aracId,parcaliSiparisSayisi)
            print "%d 'li musterinin %d nolu aractaki parcali siparis sayisi = %d" %(customerId,aracId,musteri)
            yuzdeler.append(self.percentage(musteri,parcaliSiparisSayisi))
        musteriOrani = statistics.mean(yuzdeler)
        print "%d idli musterinin orani %f"%(customerId,musteriOrani)
        return musteriOrani

    def aractakiMusteriDagilimi(self,aracId):
        conn = pymssql.connect(host='127.0.0.1', port='49865', user='sa', password='berna', database='gr2014')
        cur = conn.cursor()
        cur.execute('select count(*), sip.CARIID from SEVK sevk, SIPARIS sip where sevk.SIPID = sip.ID and sevk.FISNO = %d group by sip.CARIID', aracId)
        row = cur.fetchone()
        data = []
        while row:
            parcaliSiparis = row[0]
            musteriId = row[1]
            data.append(parcaliSiparis)
            print "%d id'li musterinin aractaki parcali siparis sayisi = %d" %(musteriId, parcaliSiparis)
            row = cur.fetchone()
        conn.close()
        return data







def main():
    a = Arac()
    customerId =1652
    #a.musterininParcaliSiparisYuzdesi(customerId)
    d = a.aractakiMusteriDagilimi(1372)
    print "Minimum parcali siparis sayisi = %d " %min(d)
    print "Maximum parcali siparis sayisi = %d " %max(d)
    print "Parcali siparis ortlamasi = %f" %statistics.mean(d)
    print "Parcali siparis standart sapmasi = %f" %statistics.stdev(d)
if  __name__ =='__main__':main()