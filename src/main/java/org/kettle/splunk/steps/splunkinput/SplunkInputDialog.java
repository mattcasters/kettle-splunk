package org.kettle.splunk.steps.splunkinput;

import com.splunk.Args;
import com.splunk.JobArgs;
import com.splunk.ResultsReaderXml;
import com.splunk.Service;
import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.kettle.splunk.connection.SplunkConnection;
import org.kettle.splunk.connection.SplunkConnectionUtil;
import org.kettle.splunk.metastore.MetaStoreFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SplunkInputDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = SplunkInputMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;

  private CCombo wConnection;

  private Text wQuery;

  private TableView wReturns;

  private SplunkInputMeta input;

  public SplunkInputDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) inputMetadata, transMeta, stepname );
    input = (SplunkInputMeta) inputMetadata;

    // Hack the metastore...
    //
    metaStore = Spoon.getInstance().getMetaStore();
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    FormLayout shellLayout = new FormLayout();
    shell.setLayout( shellLayout );
    shell.setText( "Splunk Input" );

    ModifyListener lsMod = e -> input.setChanged();
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL | SWT.H_SCROLL );
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout( scFormLayout );
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment( 0, 0 );
    fdSComposite.right = new FormAttachment( 100, 0 );
    fdSComposite.top = new FormAttachment( 0, 0 );
    fdSComposite.bottom = new FormAttachment( 100, 0 );
    wScrolledComposite.setLayoutData( fdSComposite );

    Composite wComposite = new Composite( wScrolledComposite, SWT.NONE );
    props.setLook( wComposite );
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment( 0, 0 );
    fdComposite.right = new FormAttachment( 100, 0 );
    fdComposite.top = new FormAttachment( 0, 0 );
    fdComposite.bottom = new FormAttachment( 100, 0 );
    wComposite.setLayoutData( fdComposite );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout( formLayout );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    //
    Label wlStepname = new Label( wComposite, SWT.RIGHT );
    wlStepname.setText( "Step name" );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;


    Label wlConnection = new Label( wComposite, SWT.RIGHT );
    wlConnection.setText( "Splunk Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2 * margin );
    wlConnection.setLayoutData( fdlConnection );

    Button wEditConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wEditConnection.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdEditConnection = new FormData();
    fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdEditConnection.right = new FormAttachment( 100, 0 );
    wEditConnection.setLayoutData( fdEditConnection );

    Button wNewConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wNewConnection.setText( BaseMessages.getString( PKG, "System.Button.New" ) );
    FormData fdNewConnection = new FormData();
    fdNewConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdNewConnection.right = new FormAttachment( wEditConnection, -margin );
    wNewConnection.setLayoutData( fdNewConnection );

    wConnection = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.right = new FormAttachment( wNewConnection, -margin );
    fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    wConnection.setLayoutData( fdConnection );
    lastControl = wConnection;

    Label wlQuery = new Label( wComposite, SWT.LEFT );
    wlQuery.setText( "Query:" );
    props.setLook( wlQuery );
    FormData fdlQuery = new FormData();
    fdlQuery.left = new FormAttachment( 0, 0 );
    fdlQuery.right = new FormAttachment( middle, -margin );
    fdlQuery.top = new FormAttachment( lastControl, margin );
    wlQuery.setLayoutData( fdlQuery );
    wQuery = new Text( wComposite, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL );
    wQuery.setFont( GUIResource.getInstance().getFontFixed() );
    props.setLook( wQuery );
    wQuery.addModifyListener( lsMod );
    FormData fdQuery = new FormData();
    fdQuery.left = new FormAttachment( 0, 0 );
    fdQuery.right = new FormAttachment( 100, 0 );
    fdQuery.top = new FormAttachment( wlQuery, margin );
    fdQuery.bottom = new FormAttachment( 60, 0 );
    wQuery.setLayoutData( fdQuery );
    lastControl = wQuery;


    // Some buttons
    wOK = new Button( wComposite, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wPreview = new Button( wComposite, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wCancel = new Button( wComposite, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK, wPreview, wCancel }, margin, null );


    // Table: return field name and type
    //
    ColumnInfo[] returnColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Field name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Splunk name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Return type", ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
        new ColumnInfo( "Length", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Format", ColumnInfo.COLUMN_TYPE_TEXT, false ),
      };

    Label wlReturns = new Label( wComposite, SWT.LEFT );
    wlReturns.setText( "Returns" );
    props.setLook( wlReturns );
    FormData fdlReturns = new FormData();
    fdlReturns.left = new FormAttachment( 0, 0 );
    fdlReturns.right = new FormAttachment( middle, -margin );
    fdlReturns.top = new FormAttachment( lastControl, margin );
    wlReturns.setLayoutData( fdlReturns );

    Button wbGetReturnFields = new Button( wComposite, SWT.PUSH );
    wbGetReturnFields.setText( "Get Output Fields" );
    FormData fdbGetReturnFields = new FormData();
    fdbGetReturnFields.right = new FormAttachment( 100, 0 );
    fdbGetReturnFields.top = new FormAttachment( wlReturns, margin );
    wbGetReturnFields.setLayoutData( fdbGetReturnFields );
    wbGetReturnFields.addListener( SWT.Selection, ( e ) -> getReturnValues() );

    wReturns = new TableView( transMeta, wComposite, SWT.FULL_SELECTION | SWT.MULTI, returnColumns, input.getReturnValues().size(), lsMod, props );
    props.setLook( wReturns );
    wReturns.addModifyListener( lsMod );
    FormData fdReturns = new FormData();
    fdReturns.left = new FormAttachment( 0, 0 );
    fdReturns.right = new FormAttachment( wbGetReturnFields, 0 );
    fdReturns.top = new FormAttachment( wlReturns, margin );
    fdReturns.bottom = new FormAttachment( wlReturns, 300 + margin );
    wReturns.setLayoutData( fdReturns );
    // lastControl = wReturns;

    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent( wComposite );

    wScrolledComposite.setExpandHorizontal( true );
    wScrolledComposite.setExpandVertical( true );
    wScrolledComposite.setMinWidth( bounds.width );
    wScrolledComposite.setMinHeight( bounds.height );

    // Add listeners
    //
    wCancel.addListener( SWT.Selection, e -> cancel() );
    wOK.addListener( SWT.Selection, e -> ok() );
    wPreview.addListener( SWT.Selection, e -> preview() );

    lsDef = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wConnection.addSelectionListener( lsDef );
    wStepname.addSelectionListener( lsDef );

    wNewConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        newConnection();
      }
    } );
    wEditConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        editConnection();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;

  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  public void getData() {

    wStepname.setText( Const.NVL( stepname, "" ) );
    wConnection.setText( Const.NVL( input.getConnectionName(), "" ) );

    // List of connections...
    //
    try {
      List<String> elementNames = SplunkConnectionUtil.getConnectionFactory( metaStore ).getElementNames();
      Collections.sort( elementNames );
      wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list Splunk connections", e );
    }

    wQuery.setText( Const.NVL( input.getQuery(), "" ) );

    for ( int i = 0; i < input.getReturnValues().size(); i++ ) {
      ReturnValue returnValue = input.getReturnValues().get( i );
      TableItem item = wReturns.table.getItem( i );
      item.setText( 1, Const.NVL( returnValue.getName(), "" ) );
      item.setText( 2, Const.NVL( returnValue.getSplunkName(), "" ) );
      item.setText( 3, Const.NVL( returnValue.getType(), "" ) );
      item.setText( 4, returnValue.getLength() < 0 ? "" : Integer.toString( returnValue.getLength() ) );
      item.setText( 5, Const.NVL( returnValue.getFormat(), "" ) );
    }
    wReturns.removeEmptyRows();
    wReturns.setRowNums();
    wReturns.optWidth( true );

  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    getInfo( input );
    dispose();
  }

  private void getInfo( SplunkInputMeta meta ) {
    meta.setConnectionName( wConnection.getText() );
    meta.setQuery( wQuery.getText() );

    List<ReturnValue> returnValues = new ArrayList<>();
    for ( int i = 0; i < wReturns.nrNonEmpty(); i++ ) {
      TableItem item = wReturns.getNonEmpty( i );
      String name = item.getText( 1 );
      String splunkName = item.getText( 2 );
      String type = item.getText( 3 );
      int length = Const.toInt( item.getText( 4 ), -1 );
      String format = item.getText( 5 );
      returnValues.add( new ReturnValue( name, splunkName, type, length, format ) );
    }
    meta.setReturnValues( returnValues );
  }

  protected void newConnection() {
    SplunkConnection connection = SplunkConnectionUtil.newConnection( shell, transMeta, SplunkConnectionUtil.getConnectionFactory( metaStore ) );
    if ( connection != null ) {
      wConnection.setText( connection.getName() );
    }
  }

  protected void editConnection() {
    SplunkConnectionUtil.editConnection( shell, transMeta, SplunkConnectionUtil.getConnectionFactory( metaStore ), wConnection.getText() );
  }

  private synchronized void preview() {
    SplunkInputMeta oneMeta = new SplunkInputMeta();
    this.getInfo( oneMeta );
    TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation( this.transMeta, oneMeta, this.wStepname.getText() );
    this.transMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    previewMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    EnterNumberDialog
      numberDialog = new EnterNumberDialog( this.shell, this.props.getDefaultPreviewSize(),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogTitle" ),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogMessage" )
    );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog( this.shell, previewMeta, new String[] { this.wStepname.getText() }, new int[] { previewSize } );
      progressDialog.open();
      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();
      if ( !progressDialog.isCancelled() && trans.getResult() != null && trans.getResult().getNrErrors() > 0L ) {
        EnterTextDialog etd = new EnterTextDialog( this.shell,
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title", new String[ 0 ] ),
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message", new String[ 0 ] ), loggingText, true );
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd = new PreviewRowsDialog( this.shell, this.transMeta, 0, this.wStepname.getText(), progressDialog.getPreviewRowsMeta( this.wStepname.getText() ),
        progressDialog.getPreviewRows( this.wStepname.getText() ), loggingText );
      prd.open();
    }
  }

  private void getReturnValues() {

    try {

      MetaStoreFactory<SplunkConnection> factory = SplunkConnectionUtil.getConnectionFactory( metaStore );
      SplunkConnection splunkConnection = factory.loadElement( wConnection.getText() );
      splunkConnection.initializeVariablesFrom( transMeta );
      Service service = Service.connect( splunkConnection.getServiceArgs() );
      Args args = new Args();
      args.put("connection_mode", JobArgs.ExecutionMode.BLOCKING.name());
      InputStream eventsStream = service.oneshotSearch( transMeta.environmentSubstitute( wQuery.getText() ), args );

      Set<String> detectedKeys = new HashSet<>();
      try {
        ResultsReaderXml resultsReader = new ResultsReaderXml(eventsStream);
        HashMap<String, String> event;
        while ((event = resultsReader.getNextEvent()) != null) {
          for (String key: event.keySet()) {
            detectedKeys.add( key );
          }
        }
      } finally {
        eventsStream.close();
      }

      for (String detectedKey : detectedKeys) {
        TableItem item = new TableItem(wReturns.table, SWT.NONE);
        item.setText(1, detectedKey);
        item.setText(2, detectedKey);
        item.setText(3, "String");
      }
      wReturns.removeEmptyRows();
      wReturns.setRowNums();
      wReturns.optWidth( true );

    } catch(Exception e) {
      new ErrorDialog(shell, "Error", "Error getting fields from Splunk query", e);
    }
  }
}
