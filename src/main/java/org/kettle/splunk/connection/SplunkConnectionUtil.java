package org.kettle.splunk.connection;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.kettle.splunk.metastore.MetaStoreFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;

public class SplunkConnectionUtil {
  private static Class<?> PKG = SplunkConnectionUtil.class; // for i18n purposes, needed by Translator2!!

  public static MetaStoreFactory<SplunkConnection> getConnectionFactory( IMetaStore metaStore ) {
    return new MetaStoreFactory<SplunkConnection>( SplunkConnection.class, metaStore, PentahoDefaults.NAMESPACE );
  }

  public static SplunkConnection newConnection( Shell shell, VariableSpace space, MetaStoreFactory<SplunkConnection> factory ) {

    SplunkConnection connection = new SplunkConnection( space );
    boolean ok = false;
    while ( !ok ) {
      SplunkConnectionDialog dialog = new SplunkConnectionDialog( shell, connection );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( connection.getName() ) != null ) {
            MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ConnectionExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ConnectionExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( connection );
              ok = true;
            }
          } else {
            factory.saveElement( connection );
            ok = true;
          }
        } catch ( Exception exception ) {
          new ErrorDialog( shell,
            BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ErrorSavingConnection.Title" ),
            BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ErrorSavingConnection.Message" ),
            exception );
          return null;
        }
      } else {
        // Cancel
        return null;
      }
    }
    return connection;
  }

  public static void editConnection( Shell shell, VariableSpace space, MetaStoreFactory<SplunkConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    try {
      SplunkConnection SplunkConnection = factory.loadElement( connectionName );
      SplunkConnection.initializeVariablesFrom( space );
      if ( SplunkConnection == null ) {
        newConnection( shell, space, factory );
      } else {
        SplunkConnectionDialog SplunkConnectionDialog = new SplunkConnectionDialog( shell, SplunkConnection );
        if ( SplunkConnectionDialog.open() ) {
          factory.saveElement( SplunkConnection );
        }
      }
    } catch ( Exception exception ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ErrorEditingConnection.Title" ),
        BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ErrorEditingConnection.Message" ),
        exception );
    }
  }

  public static void deleteConnection( Shell shell, MetaStoreFactory<SplunkConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }

    MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
    box.setText( BaseMessages.getString( PKG, "SplunkConnectionUtil.DeleteConnectionConfirmation.Title" ) );
    box.setMessage( BaseMessages.getString( PKG, "SplunkConnectionUtil.DeleteConnectionConfirmation.Message", connectionName ) );
    int answer = box.open();
    if ( ( answer & SWT.YES ) != 0 ) {
      try {
        factory.deleteElement( connectionName );
      } catch ( Exception exception ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ErrorDeletingConnection.Title" ),
          BaseMessages.getString( PKG, "SplunkConnectionUtil.Error.ErrorDeletingConnection.Message", connectionName ),
          exception );
      }
    }
  }

}
