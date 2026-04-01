import axios from "axios";
import { cognitoConfig, getHostedUIUrl, getLogoutUrl } from "./authConfig";

const TOKEN_ENDPOINT = `https://${cognitoConfig.domain}.auth.${cognitoConfig.region}.amazoncognito.com/oauth2/token`;
const USERINFO_ENDPOINT = `https://${cognitoConfig.domain}.auth.${cognitoConfig.region}.amazoncognito.com/oauth2/userInfo`;

class AuthService {
  login() {
    window.location.href = getHostedUIUrl();
  }

  async handleCallback(code) {
    const response = await axios.post(
      TOKEN_ENDPOINT,
      new URLSearchParams({
        grant_type: "authorization_code",
        client_id: cognitoConfig.userPoolClientId,
        code,
        redirect_uri: cognitoConfig.redirectSignIn,
      }),
      { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
    );

    const { access_token, id_token, refresh_token } = response.data;
    const user = await this.getUserInfo(access_token);

    return { token: id_token, accessToken: access_token, refreshToken: refresh_token, user };
  }

  async getUserInfo(accessToken) {
    const response = await axios.get(USERINFO_ENDPOINT, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });
    return {
      email: response.data.email,
      username: response.data.username || response.data.email,
      ...response.data,
    };
  }

  logout() {
    localStorage.removeItem("auth");
    window.location.href = getLogoutUrl();
  }

  getStoredAuth() {
    try {
      const raw = localStorage.getItem("auth");
      return raw ? JSON.parse(raw) : null;
    } catch {
      return null;
    }
  }

  storeAuth(authData) {
    localStorage.setItem("auth", JSON.stringify(authData));
  }
}

export default new AuthService();
