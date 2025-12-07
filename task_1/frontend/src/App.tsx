import React, { useEffect } from 'react';
import { ReactKeycloakProvider } from '@react-keycloak/web';
import Keycloak, { KeycloakConfig } from 'keycloak-js';
import ReportPage from './components/ReportPage';

const keycloakConfig: KeycloakConfig = {
  url: process.env.REACT_APP_KEYCLOAK_URL,
  realm: process.env.REACT_APP_KEYCLOAK_REALM||"",
  clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID||""
};

const keycloak = new Keycloak(keycloakConfig);

// Точно PKCE?
const logPKCEInfo = () => {
  if (keycloak) {
    console.log('🔐 PKCE Configuration:', {
      pkceMethod: 'S256',
      clientId: keycloakConfig.clientId,
      realm: keycloakConfig.realm
    });
    
    const originalLogin = keycloak.login;
    keycloak.login = function(options?: any) {
      console.log('🔍 PKCE Login initiated with options:', options);
      return originalLogin.call(this, options);
    };
  }
};

logPKCEInfo();

const App: React.FC = () => {
  useEffect(() => {
    console.log('PKCE enabled in initOptions: S256');
  }, []);

  return (
    <ReactKeycloakProvider 
      authClient={keycloak}
      initOptions={{
        pkceMethod: "S256"
      }}
    >
      <div className="App">
        <ReportPage />
      </div>
    </ReactKeycloakProvider>
  );
};

export default App;