const API_BASE = process.env.REACT_APP_BACKEND_URL;

/**
 * Retourne un identifiant stable pour l'utilisateur courant.
 * Essaie de décoder le sub du JWT, sinon génère/récupère un deviceId dans localStorage.
 */
export function getOrCreateUserId(): string {
  const jwt = localStorage.getItem('jwt');
  if (jwt) {
    try {
      const payload = JSON.parse(atob(jwt.split('.')[1]));
      if (payload.sub) return payload.sub;
    } catch {
      // JWT mal formé, on continue
    }
  }
  let deviceId = localStorage.getItem('deviceId');
  if (!deviceId) {
    deviceId = crypto.randomUUID();
    localStorage.setItem('deviceId', deviceId);
  }
  return deviceId;
}

export async function updateProjectAutoImport(projectId: string, autoImport: boolean): Promise<void> {
  await fetch(`${API_BASE}/api/project/${projectId}/preferences`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ auto_import: autoImport }),
  });
}

export async function getUserPreferences(userId: string): Promise<{ auto_import_always: boolean }> {
  try {
    const res = await fetch(`${API_BASE}/api/user/preferences?user_id=${encodeURIComponent(userId)}`);
    if (!res.ok) return { auto_import_always: false };
    return res.json();
  } catch {
    return { auto_import_always: false };
  }
}

export async function updateUserAutoImportAlways(userId: string, value: boolean): Promise<void> {
  await fetch(`${API_BASE}/api/user/preferences`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ user_id: userId, auto_import_always: value }),
  });
}
