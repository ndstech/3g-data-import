// timescaledb-parallel-copy loads data from CSV format into a TimescaleDB database
package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Flag vars
var (
	postgresConnect string
	dbName          string
	schemaName      string
	tableName       string
	truncate        bool

	copyOptions    string
	splitCharacter string
	fromFile       string
	columns        string

	workers         int
	batchSize       int
	logBatches      bool
	reportingPeriod time.Duration
	verbose         bool

	columnCount int64
	rowCount    int64

	durasi time.Time
	
	f *os.File
)

type batch struct {
	rows []string
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

// Parse args
func init() {
	flag.StringVar(&postgresConnect, "connection", "host=localhost user=postgres sslmode=disable", "PostgreSQL connection url")
	flag.StringVar(&dbName, "db-name", "test", "Database where the destination table exists")
	flag.StringVar(&tableName, "table", "test_table", "Destination table for insertions")
	flag.StringVar(&schemaName, "schema", "public", "Desination table's schema")
	flag.BoolVar(&truncate, "truncate", false, "Truncate the destination table before insert")

	flag.StringVar(&copyOptions, "copy-options", "", "Additional options to pass to COPY (ex. NULL 'NULL')")
	flag.StringVar(&splitCharacter, "split", ",", "Character to split by")
	flag.StringVar(&fromFile, "file", "", "File to read from rather than stdin")
	flag.StringVar(&columns, "columns", "", "Comma-separated columns present in CSV")

	flag.IntVar(&batchSize, "batch-size", 5000, "Number of rows per insert")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make")
	flag.BoolVar(&logBatches, "log-batches", false, "Whether to time individual batches.")
	flag.DurationVar(&reportingPeriod, "reporting-period", 0*time.Second, "Period to report insert stats; if 0s, intermediate results will not be reported")
	flag.BoolVar(&verbose, "verbose", false, "Print more information about copying statistics")

	flag.Parse()
}

func getConnectString() string {
	return fmt.Sprintf("%s dbname=%s", postgresConnect, dbName)
}

func getFullTableName() string {
	return fmt.Sprintf("\"%s\".\"%s\"", schemaName, tableName)
}

func main() {

	f, _ = os.OpenFile("logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()

	if truncate { // Remove existing data from the table
		dbBench := sqlx.MustConnect("postgres", getConnectString())
		_, err := dbBench.Exec(fmt.Sprintf("TRUNCATE %s", getFullTableName()))
		if err != nil {
			panic(err)
		}

		err = dbBench.Close()
		if err != nil {
			panic(err)
		}
	}

	var scanner *bufio.Scanner
	if len(fromFile) > 0 {
		file, err := os.Open(fromFile)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner = bufio.NewScanner(file)
	} else {
		scanner = bufio.NewScanner(os.Stdin)
	}

	var wg sync.WaitGroup
	batchChan := make(chan *batch, workers)

	// Generate COPY workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go processBatches(&wg, batchChan)
	}

	// Reporting thread
	if reportingPeriod > (0 * time.Second) {
		go report()
	}

	start := time.Now()
	rowsRead := scan(batchSize, scanner, batchChan)
	close(batchChan)
	wg.Wait()
	end := time.Now()
	took := end.Sub(start)
	rowRate := float64(rowsRead) / float64(took.Seconds())

	res := fmt.Sprintf("COPY %d", rowsRead)
	if verbose {
		res += fmt.Sprintf(", took %v with %d worker(s) (mean rate %f/sec)", took, workers, rowRate)
	}
	fmt.Println(res)
	
	startMoving := time.Now()
	fmt.Println("Moving data into hourly table")
	dbBench := sqlx.MustConnect("postgres", getConnectString())
	defer dbBench.Close()
	dbBench.MustExec("insert into counter_3g_hourly select * from counter_3g_lastday on conflict do nothing;")
	dbBench.MustExec("insert into counter_3g_daily select time_bucket('1 day',resulttime) tanggal, UNIQUE_ID,RNC,CELLNAME,CI,sum(VSRRCSetupConnEstab),sum(RRCSuccConnEstabsum),sum(RRCAttConnEstabOrgConvCall),sum(RRCAttConnEstabOrgStrCall),sum(RRCAttConnEstabOrgInterCall),sum(RRCAttConnEstabOrgBkgCall),sum(RRCAttConnEstabOrgSubCall),sum(RRCAttConnEstabTmConvCall),sum(RRCAttConnEstabTmStrCall),sum(RRCAttConnEstabTmInterCall),sum(RRCAttConnEstabTmBkgCall),sum(RRCAttConnEstabEmgCall),sum(RRCAttConnEstabOrgHhPrSig),sum(RRCAttConnEstabOrgLwPrSig),sum(RRCAttConnEstabCallReEst),sum(RRCAttConnEstabTmHhPrSig),sum(RRCAttConnEstabTmLwPrSig),sum(RRCAttConnEstabUnknown),sum(RRCSuccConnEstabOrgConvCall),sum(RRCSuccConnEstabOrgStrCall),sum(RRCSuccConnEstabOrgInterCall),sum(RRCSuccConnEstabOrgBkgCall),sum(RRCSuccConnEstabOrgSubCall),sum(RRCSuccConnEstabTmConvCall),sum(RRCSuccConnEstabTmStrCall),sum(RRCSuccConnEstabTmItrCall),sum(RRCSuccConnEstabTmBkgCall),sum(RRCSuccConnEstabEmgCall),sum(RRCSuccConnEstabOrgHhPrSig),sum(RRCSuccConnEstabOrgLwPrSig),sum(RRCSuccConnEstabCallReEst),sum(RRCSuccConnEstabTmHhPrSig),sum(RRCSuccConnEstabTmLwPrSig),sum(RRCSuccConnEstabUnkown),sum(VSRRCEstabDRDOutAtt),sum(VSRRCAttConnEstabSum),sum(VSRRCEstabDRDIn),sum(VSTPUE0),sum(VSTPUE1),sum(VSTPUE2),sum(VSTPUE3),sum(VSTPUE4),sum(VSTPUE5),sum(VSTPUE69),sum(VSTPUE1625),sum(VSTPUE2635),sum(VSTPUE3655),sum(VSTPUEMore55),sum(VSTPUE1015),sum(VSEcNoMeanTP0),sum(VSEcNoMeanTP1),sum(VSEcNoMeanTP2),sum(VSEcNoMeanTP3),sum(VSEcNoMeanTP4),sum(VSEcNoMeanTP5),sum(VSEcNoMeanTP69),sum(VSEcNoMeanTP1015),sum(VSEcNoMeanTP1625),sum(VSEcNoMeanTP2635),sum(VSEcNoMeanTP3655),sum(VSEcNoMeanTPMore55),sum(VSRRCRejSum),sum(VSRRCFailConnEstabCong),sum(VSRRCRejCodeCong),sum(VSRRCRejRLFail),sum(VSRRCRejTNLFail),sum(VSRRCRejRedirIntraRat),sum(VSRRCRejRedirInterRat),sum(VSRRCFailConnEstabNoReply),sum(VSRRCRejULCECong),sum(VSRRCRejDLCECong),sum(VSRRCRejULIUBBandCong),sum(VSRRCRejDLIUBBandCong),sum(VSRRCRejULPowerCong),sum(VSRRCRejDLPowerCong),sum(VSRRCRejRedirService),sum(VSLowPriRRCRanFCDiscNum),sum(VSNormPriRRCRanFCDiscNum),sum(VSHighPriRRCRanFCDiscNum),sum(VSRRCRejRedirDist),sum(VSRRCFCDiscNum),sum(VSRRCRejRedirDistIntraRat),sum(VSRRCRejNodeBResUnavail),sum(VSRRCRejRedirCoMacroMicro),sum(VSRRCRejRedirPingPongNum),sum(VSRRCRejNodeBULCECong),sum(VSRRCRejNodeBDLCECong),sum(VSRRCRejRedirWeakCoverage),sum(VSRRCFailConnEstabNoReplyCSFB),sum(VSRABAttEstabCSConv),sum(VSRABAttEstabCSStr),sum(VSRABSuccEstabCSConv),sum(VSRABSuccEstabCSStr),sum(VSRABAttEstabAMR),sum(VSRABAttEstCSConv64),sum(VSRABSuccEstabCSAMR),sum(VSRABSuccEstCSConv64),sum(VSRABSuccEstabCSAMR122),sum(VSRABAttEstabCSVPLimit),sum(VSRABAttEstabCSQueue),sum(VSRABEstabQueueTimeCS),sum(VSRABSuccEstabCSQueue),sum(VSRABFailEstabCSUnsp),sum(VSRABFailEstabCSCodeCong),sum(VSRABFailEstabCSCong),sum(VSRABFailEstabCSRNL),sum(VSRABFailEstabCSTNL),sum(VSRABFailEstabCSULCECong),sum(VSRABFailEstabCSDLCECong),sum(VSRABFailEstabCSDLIUBBandCong),sum(VSRABFailEstabCSULIUBBandCong),sum(VSRABFailEstabCSRBIncCfg),sum(VSRABFailEstabCSRBCfgUnsup),sum(VSRABFailEstabCSPhyChFail),sum(VSRABFailEstabCSUuNoReply),sum(VSRABFailEstabCSULPowerCong),sum(VSRABFailEstabCSDLPowerCong),sum(VSRABFailEstabCSIubFail),sum(VSRABFailEstabCSUuFail),sum(VSRABFailEstabCSSRBReset),sum(VSRABFailEstabCSCellUpd),sum(VSRABFailEstabCSNodeBULCECong),sum(VSRABFailEstabCSNodeBDLCECong),sum(VSRABFailEstabCSDLIUCSBandCong),sum(VSRABFailEstabCSULIUCSBandCong),sum(VSRABFailEstabCSIubAAL2Fail),sum(VSRABFailEstabCSSRBResetCSFB),sum(VSRABFailEstabCSCellUpdCSFB),sum(VSRABFailEstabCSUuFailCSFB),sum(VSRABAttRelCSNormRel),sum(VSRABAttRelCSUEInact),sum(VSRABAttRelCSPreempt),sum(VSRABAttRelCSOM),sum(VSRABAttRelCSNetOpt),sum(VSRABAttRelCSUTRANGen),sum(VSCNRABLossCS),sum(VSRABAttRelCS),sum(VSRABAttEstabPSConv),sum(VSRABAttEstabPSStr),sum(VSRABAttEstabPSInt),sum(VSRABAttEstabPSBkg),sum(VSRABSuccEstabPSConv),sum(VSRABSuccEstabPSStr),sum(VSRABSuccEstabPSInt),sum(VSRABSuccEstabPSBkg),sum(VSRABSuccEstabPS0kbps),sum(VSRABAttEstabPSQueue),sum(VSRABEstabQueueTimePS),sum(VSRABSuccEstabPSQueue),sum(VSRABSuccEstabPSPTT),sum(VSRABAttEstabPSPTT),sum(VSRABSuccEstabPSR99),sum(VSRABAttEstabPSR99),sum(VSRABAttEstabPSFree),sum(VSRABSuccEstabPSFree),sum(VSRABFailEstabPSUnsp),sum(VSRABFailEstabPSCodeCong),sum(VSRABFailEstabPSRNL),sum(VSRABFailEstabPSTNL),sum(VSRABFailEstabPSULCECong),sum(VSRABFailEstabPSDLCECong),sum(VSRABFailEstabPSDLIUBBandCong),sum(VSRABFailEstabPSULIUBBandCong),sum(VSRABFailEstabPSRBIncCfg),sum(VSRABFailEstabPSRBCfgUnsupp),sum(VSRABFailEstabPSPhyChFail),sum(VSRABFailEstabPSUuNoReply),sum(VSRABFailEstabPSULPowerCong),sum(VSRABFailEstabPSDLPowerCong),sum(VSRABFailEstabPSIubFail),sum(VSRABFailEstabPSUuFail),sum(VSRABFailEstabPSCong),sum(VSRABFailEstabPSDLPowerCongFree),sum(VSRABFailEstabPSSRBReset),sum(VSRABFailEstabPSCellUpd),sum(VSRABFailEstabPSHSUPAUserCong),sum(VSRABFailEstabPSHSDPAUserCong),sum(VSRABFailEstabPSNodeBULCECong),sum(VSRABFailEstabPSNodeBDLCECong),sum(VSRABFailEstabPSDLIUPSBandCong),sum(VSRABFailEstabPSULIUPSBandCong),sum(VSRABFailEstabPSIubAAL2Fail),sum(VSRABFailEstabPSULCEFinalCong),sum(VSRABAttRelPSNormRel),sum(VSRABAttRelPSUtranGen),sum(VSRABAttRelPSUeInact),sum(VSRABAttRelPSRABPreempt),sum(VSRABAttRelPSOM),sum(VSRABAttRelPSNetOptm),sum(VSRABAttRelPSUnsp),sum(VSCNRABLossPS),sum(VSRABAttRelPS),sum(VSRABAbnormRelCSRF),sum(VSRABAbnormRelCS),sum(VSRABNormRelCS),sum(VSRABAbnormRelPSRF),sum(VSRABAbnormRelPS),sum(VSRABNormRelPS),sum(VSRABAbnormRelCSOM),sum(VSRABAbnormRelCSUTRANgen),sum(VSRABAbnormRelCSPreempt),sum(VSRABAbnormRelPSOM),sum(VSRABAbnormRelPSPreempt),sum(VSRABAbnormRelCSRFSRBReset),sum(VSRABAbnormRelPSRFSRBReset),sum(VSRABAbnormRelPSRFTRBReset),sum(VSRABAbnormRelCSIuAAL2),sum(VSRABAbnormRelPSGTPULoss),sum(VSRABAbnormRelAMR),sum(VSRABAbnormRelCS64),sum(VSRABAbnormRelCSRFULSync),sum(VSRABAbnormRelPSRFULSync),sum(VSRABAbnormRelCSRFUuNoReply),sum(VSRABAbnormRelPSRFUuNoReply),sum(VSRABNormRelAMR),sum(VSRABAbnormRelPSOLC),sum(VSRABAbnormRelCSOLC),sum(VSRABNormRelCS64),sum(VSRABNormRelPSCCH),sum(VSRABAbnormRelPSCCH),sum(VSRABNormRelPSUEGen),sum(VSRABAbnormRelCSStr),sum(VSRABAbnormRelPSConv),sum(VSRABAbnormRelPSStr),sum(VSRABNormRelCSStr),sum(VSRABNormRelPSConv),sum(VSRABNormRelPSStr),sum(VSRABRelReqPSBEHSUPACongGolden),sum(VSRABRelReqPSBEHSUPACongSilver),sum(VSRABAbnormRelCSHSPAConv),sum(VSRABNormRelCSHSPAConv),sum(VSRABNormRelVPLimit),sum(VSRABNormRelPS0kbpsTimeout),sum(VSRABNormRelCSUEGen),sum(VSRABNormRelPSBE),sum(VSRABAbnormRelPSBE),sum(VSRABAbnormRelCS64RF),sum(VSRABAbnormRelPSPTT),sum(VSRABNormRelPSPTT),sum(VSRABAbnormRelPSR99RF),sum(VSRABAbnormRelPSR99),sum(VSRABNormRelPSR99),sum(VSRABNormRelPSPCH),sum(VSRABAbnormRelPSPCH),sum(VSRABAbnormRelPSF2P),sum(VSRABAbnormRelPSD2P),sum(VSRABAbnormRelPSR99D2P),sum(VSRABSFOccupyMAX),sum(VSMultRABSF8),sum(VSMultRABSF16),sum(VSMultRABSF32),sum(VSMultRABSF64),sum(VSSingleRABSF4),sum(VSSingleRABSF8),sum(VSSingleRABSF16),sum(VSSingleRABSF32),sum(VSSingleRABSF64),sum(VSSingleRABSF128),sum(VSSingleRABSF256),sum(VSMultRABSF4),sum(VSMultRABSF128),sum(VSMultRABSF256),sum(VSRABSFOccupy),sum(VSDRDRBSetupAttOut),sum(VSDRDRBSetupSuccOut),sum(VSDRDRBSetupAttIn),sum(VSDRDRBSetupSuccIn),sum(VSRBCSConvDL64),sum(VSRBPSIntDL8),sum(VSRBPSIntDL16),sum(VSRBPSIntDL32),sum(VSRBPSIntDL64),sum(VSRBPSIntDL128),sum(VSRBPSIntDL144),sum(VSRBPSIntDL256),sum(VSRBPSIntDL384),sum(VSRBPSIntUL8),sum(VSRBPSIntUL16),sum(VSRBPSIntUL32),sum(VSRBPSIntUL64),sum(VSRBPSIntUL128),sum(VSRBPSIntUL144),sum(VSRBPSIntUL256),sum(VSRBPSIntUL384),sum(VSRBPSBkgDL8),sum(VSRBPSBkgDL16),sum(VSRBPSBkgDL32),sum(VSRBPSBkgDL64),sum(VSRBPSBkgDL128),sum(VSRBPSBkgDL144),sum(VSRBPSBkgDL256),sum(VSRBPSBkgDL384),sum(VSRBPSBkgUL8),sum(VSRBPSBkgUL16),sum(VSRBPSBkgUL32),sum(VSRBPSBkgUL64),sum(VSRBPSBkgUL128),sum(VSRBPSBkgUL144),sum(VSRBPSBkgUL256),sum(VSRBPSBkgUL384),sum(VSRBAMRDL122),sum(VSSHOAttRLAdd),sum(VSSHOSuccRLAdd),sum(VSSHOFailRLAddCfgUnsupp),sum(VSSHOFailRLAddISR),sum(VSSHOFailRLAddInvCfg),sum(VSSHOFailRLAddNoReply),sum(VSSHOAttRLDel),sum(VSSHOSuccRLDel),sum(VSSHOAS1RL),sum(VSSHOAS2RL),sum(VSSHOAS3RL),sum(VSSHOAS4RL),sum(VSSHOAS5RL),sum(VSSHOAS6RL),sum(VSHHOAttInterFreqOut),sum(VSHHOSuccInterFreqOut),sum(VSHHOFailInterFreqOutCfgUnsupp),sum(VSHHOFailInterFreqOutPyhChFail),sum(VSHHOFailInterFreqOutISR),sum(VSHHOFailInterFreqOutCellUpdt),sum(VSHHOFailInterFreqOutInvCfg),sum(VSHHOFailInterFreqOutNoReply),sum(VSHHOFailInterFreqOutPrepFail),sum(VSHHOFailInterFreqOutRLSetupFail),sum(IRATHOAttRelocPrepOutCS),sum(IRATHOSuccRelocPrepOutCS),sum(IRATHOAttOutCS),sum(IRATHOSuccOutCS),sum(IRATHOFailOutCSCfgUnsupp),sum(IRATHOFailOutCSPhyChFail),sum(IRATHOAttOutPSUTRAN),sum(IRATHOSuccOutPSUTRAN),sum(IRATHOFailOutPSUTRANCfgUnsupp),sum(IRATHOFailOutPSUTRANPhyChFail),sum(VSIRATHOFailOutCSNoReply),sum(VSIRATHOFailOutPSUTRANNoReply),sum(VSIRATHOAttOutCSTrigRscp),sum(VSIRATHOAttOutCSTrigEcNo),sum(VSIRATHOAttOutPSTrigRscp),sum(VSIRATHOAttOutPSTrigEcNo),sum(VSIRATHOSuccOutCSTrigRscp),sum(VSIRATHOSuccOutCSTrigEcNo),sum(VSIRATHOSuccOutPSTrigRscp),sum(VSIRATHOSuccOutPSTrigEcNo),sum(VSIRATHOFailOutCSAbort),sum(VSIRATHOFailOutPSAbort),sum(VSMeanRTWP),sum(VSMeanTCP),sum(VSMaxRTWP),sum(VSMinRTWP),sum(VSMaxTCP),sum(VSMinTCP),sum(VSMaxTCPNonHS),sum(VSMinTCPNonHS),sum(VSMeanTCPNonHS),sum(VSIUBAttRLSetup),sum(VSIUBAttRLAdd),sum(VSIUBAttRLRecfg),sum(VSHSDPAD2HSucc),sum(VSHSDPAF2HSucc),sum(VSHSDPAH2DSucc),sum(VSHSDPAH2FSucc),sum(VSHSDPAMeanChThroughputTotalBytes),sum(VSHSDPASHOServCellChgAttOut),sum(VSHSDPASHOServCellChgSuccOut),sum(VSHSDPARABAttEstab),sum(VSHSDPARABSuccEstab),sum(VSHSDPAHHOH2DSuccOutIntraFreq),sum(VSHSDPAHHOH2DSuccOutInterFreq),sum(VSHSDPARABNormRelUsrInact),sum(VSHSDPARABAbnormRel),sum(VSHSDPARABAbnormRelRF),sum(VSHSDPARABNormRel),sum(VSHSDPAMeanChThroughput),sum(VSHSDPAUEMeanCell),sum(VSHSDPARABFailEstabDLPowerCong),sum(VSHSDPARABFailEstabDLIUBBandCong),sum(VSHSDPAUEMaxCell),sum(VSHSDPARABDCAttEstab),sum(VSHSDPARABDCSuccEstab),sum(VSHSDPA64QAMUEMeanCell),sum(VSHSDPADCPRIMUEMeanCell),sum(VSHSDPADCSECUEMeanCell),sum(VSHSDPARABAbnormRelH2P),sum(VSLCULCreditUsedMax),sum(VSLCULCreditUsedMin),sum(VSLCDLCreditUsedMax),sum(VSLCDLCreditUsedMin),sum(VSDCCCSuccF2P),sum(VSCellUnavailTime),sum(VSLCULCreditUsedMean),sum(VSLCDLCreditUsedMean),sum(VSCellUnavailTimeSys),sum(VSDCCCD2PSucc),sum(VSHSDPAH2PSucc),sum(VSHSUPAE2PSucc),sum(VSPSR99D2PSucc),sum(VSHSUPARABAttEstab),sum(VSHSUPARABSuccEstab),sum(VSHSUPARABAbnormRel),sum(VSHSUPARABNormRel),sum(VSHSUPAE2DSucc),sum(VSHSUPAHHOE2DSuccOutIntraFreq),sum(VSHSUPAHHOE2DSuccOutInterFreq),sum(VSHSUPAE2FSucc),sum(VSHSUPAMeanChThroughputTotalBytes),sum(VSHSUPAUEMeanCell),sum(VSHSUPAMeanChThroughput),sum(VSHSUPARABFailEstabULPowerCong),sum(VSHSUPARABFailEstabULIUBBandCong),sum(VSHSUPARABFailEstabULCECong),sum(VSHSUPAUEMaxCell),sum(VSHSUPARABAbnormRelE2P),sum(VSHSUPADCPRIMUEMeanCell),sum(VSHSUPAUEMaxTTI2ms),sum(VSHSUPAUEMaxTTI10ms),sum(VSHSUPADCSECUEMeanCell),sum(VSHSUPAUEMeanTTI2ms),sum(VSHSUPAUEMeanTTI10ms),sum(VSPSBkgDL8Traffic),sum(VSPSBkgDL16Traffic),sum(VSPSBkgDL32Traffic),sum(VSPSBkgDL64Traffic),sum(VSPSBkgDL128Traffic),sum(VSPSBkgDL144Traffic),sum(VSPSBkgDL256Traffic),sum(VSPSBkgDL384Traffic),sum(VSPSIntDL8Traffic),sum(VSPSIntDL16Traffic),sum(VSPSIntDL32Traffic),sum(VSPSIntDL64Traffic),sum(VSPSIntDL128Traffic),sum(VSPSIntDL144Traffic),sum(VSPSIntDL256Traffic),sum(VSPSIntDL384Traffic),sum(VSPSStrDL32Traffic),sum(VSPSStrDL64Traffic),sum(VSPSStrDL128Traffic),sum(VSPSStrDL144Traffic),sum(VSPSBkgUL8Traffic),sum(VSPSBkgUL16Traffic),sum(VSPSBkgUL32Traffic),sum(VSPSBkgUL64Traffic),sum(VSPSBkgUL128Traffic),sum(VSPSBkgUL144Traffic),sum(VSPSBkgUL256Traffic),sum(VSPSBkgUL384Traffic),sum(VSPSIntUL8Traffic),sum(VSPSIntUL16Traffic),sum(VSPSIntUL32Traffic),sum(VSPSIntUL64Traffic),sum(VSPSIntUL128Traffic),sum(VSPSIntUL144Traffic),sum(VSPSIntUL256Traffic),sum(VSPSIntUL384Traffic),sum(VSPSStrUL16Traffic),sum(VSPSStrUL32Traffic),sum(VSPSStrUL64Traffic),sum(VSPSBkgKbpsDL8),sum(VSPSBkgKbpsDL16),sum(VSPSBkgKbpsDL32),sum(VSPSBkgKbpsDL64),sum(VSPSBkgKbpsDL128),sum(VSPSBkgKbpsDL144),sum(VSPSBkgKbpsDL256),sum(VSPSBkgKbpsDL384),sum(VSPSIntKbpsDL8),sum(VSPSIntKbpsDL16),sum(VSPSIntKbpsDL32),sum(VSPSIntKbpsDL64),sum(VSPSIntKbpsDL128),sum(VSPSIntKbpsDL144),sum(VSPSIntKbpsDL256),sum(VSPSIntKbpsDL384),sum(VSPSStrKbpsDL32),sum(VSPSStrKbpsDL64),sum(VSPSStrKbpsDL128),sum(VSPSStrKbpsDL144),sum(VSPSBkgKbpsUL8),sum(VSPSBkgKbpsUL16),sum(VSPSBkgKbpsUL32),sum(VSPSBkgKbpsUL64),sum(VSPSBkgKbpsUL128),sum(VSPSBkgKbpsUL144),sum(VSPSBkgKbpsUL256),sum(VSPSBkgKbpsUL384),sum(VSPSIntKbpsUL8),sum(VSPSIntKbpsUL16),sum(VSPSIntKbpsUL32),sum(VSPSIntKbpsUL64),sum(VSPSIntKbpsUL128),sum(VSPSIntKbpsUL144),sum(VSPSIntKbpsUL256),sum(VSPSIntKbpsUL384),sum(VSPSStrKbpsUL16),sum(VSPSStrKbpsUL32),sum(VSPSStrKbpsUL64),sum(VSRRCPaging1LossPCHCongCell),sum(VSUTRANAttPaging1),sum(VSCellDCHUEs),sum(VSCellFACHUEs),sum(VSCellPCHUEs),sum(VSDCCCSuccF2U),sum(VSDCCCSuccD2U),sum(VSIRATHOHSDPAAttOutPSUTRAN),sum(VSIRATHOHSDPASuccOutPSUTRAN),sum(VSIRATHOHSUPASuccOutPSUTRAN),sum(VSIRATHOHSUPAAttOutPSUTRAN),sum(VSCellFACHUEsMAX),sum(VSFACHDTCHCONGTIME),sum(VSFACHDCCHCONGTIME),sum(VSFACHCCCHCONGTIME),sum(VSDCCCP2DAtt),sum(VSDCCCP2DDRDAtt),sum(VSOrigCallEstabMeanTimeAMRNB),sum(VSOrigCallEstabMeanTimeAMRWB),sum(VSAMRErlangBestCell),sum(VSRBAMRWBDL1265),sum(VSRABAttEstabCSAMRWB),sum(VSRABSuccEstabCSAMRWB),sum(VSRABAbnormRelAMRWB),sum(VSRABNormRelAMRWB),sum(VSRRCFCNumFACHCong),sum(VSSuccCellUpdtOrgConvCallPCH),sum(VSSuccCellUpdtTmConvCallPCH),sum(VSSuccCellUpdtEmgCallPCH),sum(VSAttCellUpdtOrgConvCallPCH),sum(VSAttCellUpdtTmConvCallPCH),sum(VSVPErlangBestCell),sum(VSPSBEkbitsUL032BestCell),sum(VSPSBEkbitsUL3264BestCell),sum(VSPSBEkbitsUL64144BestCell),sum(VSPSBEkbitsUL144384BestCell),sum(VSSuccEstabPSAfterP2F),sum(VSAttEstabPSAfterP2F),sum(VSSuccEstabPSAfterP2D),sum(VSAttEstabPSAfterP2D),sum(VSSuccRecfgF2HDataTransTrig),sum(VSSuccRecfgP2HDataTransTrig),sum(VSAttRecfgF2HDataTransTrig),sum(VSAttRecfgP2HDataTransTrig),sum(VSSuccRecfgF2EDataTransTrig),sum(VSSuccRecfgP2EDataTransTrig),sum(VSAttRecfgF2EDataTransTrig),sum(VSAttRecfgP2EDataTransTrig),sum(VSSRNCIubBytesPSR99StrRx),sum(VSSRNCIubBytesPSR99IntRx),sum(VSSRNCIubBytesPSR99BkgRx),sum(VSSRNCIubBytesPSR99ConvRx),sum(VSCRNCIubBytesPSR99CCHRx),sum(VSSRNCIubBytesPSR99StrTx),sum(VSSRNCIubBytesPSR99IntTx),sum(VSSRNCIubBytesPSR99BkgTx),sum(VSSRNCIubBytesPSR99ConvTx),sum(VSCRNCIubBytesPSR99CCHTx),sum(VSSRNCIubBytesHSDPATx),sum(VSSRNCIubBytesPSEFACHTx),sum(VSSRNCIubBytesHSUPARx),sum(VSAttCellUpdtEmgCallPCH),sum(VSRABAbnormRelAMR795),sum(VSRABAbnormRelAMR122),sum(VSRABAbnormRelAMR59),sum(VSRABAbnormRelAMR475),sum(VSRABAbnormRelAMRRF),sum(VSRABAbnormRelCSOthers),sum(VSRABAbnormRelCSIuTNL),sum(VSRABAbnormRelCSIuupFail),sum(VSRABAbnormRelCSCN),sum(VSRABAbnormRelCSCSFB),sum(VSRABAbnormRelCSCSFBRF),sum(VSRABAbnormRelCSPlatinum),sum(VSRABAbnormRelCSSecurity),sum(VSRABAbnormRelPSOthers),sum(VSRABAbnormRelPSUTRANgen),sum(VSRABAbnormRelPSIuTNL),sum(VSRABAbnormRelPSRFOthers),sum(VSRABAbnormRelPSCN),sum(VSRABAbnormRelPSSecurity),sum(VSRABAbnormRelPSR99D2F),sum(VSRABAbnormRelPSR99CellDCHCellUpdt),sum(VSHSDPARABAbnormRel64QAM),sum(VSHSDPARABAbnormRel64QAM2P),sum(VSHSDPARABAbnormRelCellDCHCellUpdt),sum(VSHSDPARABAbnormRelDC),sum(VSHSDPARABAbnormRelDC2P),sum(VSHSDPARABAbnormRelDCMIMO2P),sum(VSHSDPARABAbnormRelH2F),sum(VSHSDPARABAbnormRelSRBoH),sum(VSHSDPARABAbnormRelSRBoHH2P),sum(VSHHOInterFreqOutCSDrop),sum(VSHHOIntraFreqOutDrop),sum(VSHHOInterFreqOutPSDrop),sum(VSRRCRejRedirIntraRatCSService),sum(VSRRCRejRedirIntraRatPSService),sum(VSRRCRejRedirInterRatCSService),sum(VSRRCRejRedirInterRatPSService),sum(VSSHOFailRLAddIubHW),sum(VSIRATHOFailOutCSCNUnspecFail),sum(VSIRATHOFailOutCSInterRatRF),sum(VSIRATHOFailOutCSSCRI),sum(VSIRATHOFailOutPSUTRANCNUnspecFail),sum(VSIRATHOFailOutPSUTRANInterRatRF),sum(VSIRATHOFailOutPSUTRANNoSRNSDataForwardCmd),sum(VSIRATHOFailOutPSUTRANSCRI),sum(VSIRATHOFailOutPS),sum(VSIRATHOFailOutPSUEGen),sum(VSHHOFailInterFreqOutInterRNCCellUpdt),sum(VSHHOFailInterFreqOutInterRNCCfgUnsupp),sum(VSHHOFailInterFreqOutInterRNCInvCfg),sum(VSHHOFailInterFreqOutInterRNCISR),sum(VSHHOFailInterFreqOutInterRNCNoReply),sum(VSHHOFailInterFreqOutInterRNCPhyChFail),sum(VSRRCPaging1PCHCongCSPreemptAtt),sum(VSIUBFailRLRecfgCong),sum(VSHSUPARABAbnormRelRF),sum(VSHSDPAD2HAtt),sum(VSHSUPAD2ESucc),sum(VSHSUPAD2EAtt),sum(VSPSBEkbitsDL032BestCell),sum(VSPSBEkbitsDL3264BestCell),sum(VSPSBEkbitsDL64144BestCell),sum(VSPSBEkbitsDL144384BestCell),sum(VSHSUPAGoldenBeMeanChThroughputTotalBytes),sum(VSHSUPASilverBeMeanChThroughputTotalBytes),sum(VSHSUPACopperBeMeanChThroughputTotalBytes),sum(VSRRCSuccConnEstabCSFB),sum(VSRRCAttConnEstabCSFB),sum(VSRABSuccEstabCSCSFBRedir),sum(VSRABAttEstabCSCSFBRedir) from counter_3g_lastday group by tanggal, UNIQUE_ID, RNC,CELLNAME, CI on conflict do nothing;")
	dbBench.MustExec("TRUNCATE counter_3g_lastday")
	endMoving := time.Now()
	movingDuration := endMoving.Sub(startMoving)
	fmt.Println(fmt.Sprintf("Data has been moved successfully in %v seconds)", movingDuration))
}

// report periodically prints the write rate in number of rows per second
func report() {
	start := time.Now()
	prevTime := start
	prevRowCount := int64(0)

	for now := range time.NewTicker(reportingPeriod).C {
		rCount := atomic.LoadInt64(&rowCount)

		took := now.Sub(prevTime)
		rowrate := float64(rCount-prevRowCount) / float64(took.Seconds())
		overallRowrate := float64(rCount) / float64(now.Sub(start).Seconds())
		totalTook := now.Sub(start)

		fmt.Printf("at %v, row rate %f/sec (period), row rate %f/sec (overall), %E total rows\n", totalTook-(totalTook%time.Second), rowrate, overallRowrate, float64(rCount))

		prevRowCount = rCount
		prevTime = now
	}

}

// scan reads lines from a bufio.Scanner, each which should be in CSV format
// with a delimiter specified by --split (comma by default)
func scan(itemsPerBatch int, scanner *bufio.Scanner, batchChan chan *batch) int64 {
	rows := make([]string, 0, itemsPerBatch)
	var linesRead int64

	for scanner.Scan() {
		linesRead++

		rows = append(rows, scanner.Text())
		if len(rows) >= itemsPerBatch { // dispatch to COPY worker & reset
			batchChan <- &batch{rows}
			rows = make([]string, 0, itemsPerBatch)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// Finished reading input, make sure last batch goes out.
	if len(rows) > 0 {
		batchChan <- &batch{rows}
	}

	return linesRead
}

// processBatches reads batches from C and writes them to the target server, while tracking stats on the write.
func processBatches(wg *sync.WaitGroup, C chan *batch) {
	dbBench := sqlx.MustConnect("postgres", getConnectString())
	defer dbBench.Close()
	columnCountWorker := int64(0)
	for batch := range C {
		start := time.Now()

		tx := dbBench.MustBegin()
		delimStr := fmt.Sprintf("'%s'", splitCharacter)
		if splitCharacter == "\\t" {
			delimStr = "E" + delimStr
		}
		var copyCmd string
		if columns != "" {
			copyCmd = fmt.Sprintf("COPY %s(%s) FROM STDIN WITH DELIMITER %s %s", getFullTableName(), columns, delimStr, copyOptions)
		} else {
			copyCmd = fmt.Sprintf("COPY %s FROM STDIN WITH DELIMITER %s %s", getFullTableName(), delimStr, copyOptions)
		}
		
		//fmt.Println(copyCmd)

		stmt, err := tx.Prepare(copyCmd)
		if err != nil {
			panic(err)
		}

		// Need to cover the string-ified version of the character to actual character for correct split
		sChar := splitCharacter
		if sChar == "\\t" {
			sChar = "\t"
		}
		
		//fmt.Println(sChar)
		//os.Exit(3)
		
		for _, line := range batch.rows {
			sp := strings.Split(line, sChar)

			var unique_id = sp[3] + sp[2]
			slice_1 := make([]string, 2)
			slice_1[0] = sp[0]
			slice_1[1] = unique_id

			var slice_2 []string = sp[1:]
			new_sp := append(slice_1, slice_2...)
			
			finalCommand := strings.Join(new_sp, ",")
			
			if _, err := f.WriteString(finalCommand + "\n+++++++++++++++++++++++++++++++++++++++++++++++++\n"); err != nil {
				log.Println(err)
			}

			columnCountWorker += int64(len(new_sp))
			// For some reason this is only needed for tab splitting
			if sChar == "\t" {
				args := make([]interface{}, len(new_sp))
				for i, v := range new_sp {
					args[i] = v
				}
				_, err = stmt.Exec(args...)
			} else {
				//fmt.Println("Masuk else")
				_, err = stmt.Exec(finalCommand)
			}

			if err != nil {
				panic(err)
			}
		}
		atomic.AddInt64(&columnCount, columnCountWorker)
		atomic.AddInt64(&rowCount, int64(len(batch.rows)))
		columnCountWorker = 0

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = tx.Commit()
		if err != nil {
			panic(err)
		}

		if logBatches {
			took := time.Now().Sub(start)
			fmt.Printf("[BATCH] took %v, batch size %d, row rate %f/sec\n", took, batchSize, float64(batchSize)/float64(took.Seconds()))
		}

	}
	wg.Done()
}
