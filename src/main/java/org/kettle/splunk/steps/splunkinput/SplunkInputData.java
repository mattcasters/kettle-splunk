package org.kettle.splunk.steps.splunkinput;

import com.splunk.Job;
import com.splunk.JobCollection;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.kettle.splunk.connection.SplunkConnection;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.io.InputStream;

public class SplunkInputData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  public SplunkConnection splunkConnection;
  public int[] fieldIndexes;
  public String query;
  public IMetaStore metaStore;

  public ServiceArgs serviceArgs;
  public Service service;
  public InputStream eventsStream;
}
