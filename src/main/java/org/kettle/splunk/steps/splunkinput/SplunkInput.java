package org.kettle.splunk.steps.splunkinput;


import com.splunk.Args;
import com.splunk.JobArgs;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.commons.lang.StringUtils;
import org.kettle.splunk.connection.SplunkConnectionUtil;
import org.kettle.splunk.metastore.MetaStoreUtil;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.exceptions.MetaStoreException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;

public class SplunkInput extends BaseStep implements StepInterface {

  private SplunkInputMeta meta;
  private SplunkInputData data;

  public SplunkInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                      TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }


  @Override public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {

    meta = (SplunkInputMeta) smi;
    data = (SplunkInputData) sdi;

    // Is the step getting input?
    //
    List<StepMeta> steps = getTransMeta().findPreviousSteps( getStepMeta() );

    // Connect to Neo4j
    //
    if ( StringUtils.isEmpty( meta.getConnectionName() ) ) {
      log.logError( "You need to specify a Splunk connection to use in this step" );
      return false;
    }
    try {
      // To correct lazy programmers who built certain PDI steps...
      //
      data.metaStore = MetaStoreUtil.findMetaStore( this );
      data.splunkConnection = SplunkConnectionUtil.getConnectionFactory( data.metaStore ).loadElement( meta.getConnectionName() );
      data.splunkConnection.initializeVariablesFrom( this );

    } catch ( MetaStoreException e ) {
      log.logError( "Could not load Splunk connection '" + meta.getConnectionName() + "' from the metastore", e );
      return false;
    }

    try {

      data.serviceArgs = data.splunkConnection.getServiceArgs();

      data.service = Service.connect( data.serviceArgs );

    } catch ( Exception e ) {
      log.logError( "Unable to get or create Neo4j database driver for database '" + data.splunkConnection.getName() + "'", e );
      return false;
    }

    return super.init( smi, sdi );
  }

  @Override public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {

    meta = (SplunkInputMeta) smi;
    data = (SplunkInputData) sdi;

    super.dispose( smi, sdi );
  }


  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    meta = (SplunkInputMeta) smi;
    data = (SplunkInputData) sdi;

    if ( first ) {
      first = false;

      // get the output fields...
      //
      data.outputRowMeta = new RowMeta();
      meta.getFields( data.outputRowMeta, getStepname(), null, getStepMeta(), this, repository, data.metaStore );

      // Run a one shot search in blocking mode
      //
      Args args = new Args();
      args.put("connection_mode", JobArgs.ExecutionMode.BLOCKING.name());

      data.eventsStream = data.service.oneshotSearch( getTransMeta().environmentSubstitute( meta.getQuery() ), args );
    }

    try {
      ResultsReaderXml resultsReader = new ResultsReaderXml( data.eventsStream );
      HashMap<String, String> event;
      while ( ( event = resultsReader.getNextEvent() ) != null ) {

        Object[] outputRow = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

        for ( int i = 0; i < meta.getReturnValues().size(); i++ ) {
          ReturnValue returnValue = meta.getReturnValues().get( i );
          String value = event.get( returnValue.getSplunkName() );
          outputRow[ i ] = value;
        }

        incrementLinesInput();
        putRow( data.outputRowMeta, outputRow );
      }

    } catch ( Exception e ) {
      throw new KettleException( "Error reading from Splunk events stream", e );
    } finally {
      try {
        data.eventsStream.close();
      } catch ( IOException e ) {
        throw new KettleException( "Unable to close events stream", e );
      }
    }

    // Nothing more
    //
    setOutputDone();
    return false;
  }
}
