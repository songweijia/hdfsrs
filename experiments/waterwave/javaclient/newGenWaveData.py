import os, sys, math, string, time
import numpy as np

m = 100;                  # grid size  
g = 9.8;                 # gravitational constant 
dt = 0.01;               # hardwired timestep 
dx = 1.0; 
dy = 1.0;
timesteps = 20;           # drop interval  

def droplet(height, width):
    ''' 
    DROPLET  2D Gaussian  
    D = droplet(height, width)
    '''     
    x, y = np.meshgrid(np.linspace(-1, 1, width), np.linspace(-1, 1, width), indexing='ij') 
    return height * np.exp(-5*(x**2 + y**2))  

def genData():
    
    # init the height matrix
    H = np.ones((m+2,m+2));   U = np.zeros((m+2,m+2));  V = np.zeros((m+2,m+2));  
    Hx = np.zeros((m+1,m+1)); Ux = np.zeros((m+1,m+1)); Vx = np.zeros((m+1,m+1));    
    Hy = np.zeros((m+1,m+1)); Uy = np.zeros((m+1,m+1)); Vy = np.zeros((m+1,m+1)); 
   
    # simulate a water drop
    D = droplet(1.5, 10) #21
    
    # water drop
    w = D.shape[0]
    midw = (m-w)/2
    i = np.s_[midw:midw+w]          
    j = np.s_[midw:midw+w]
    H[i,j] = H[i,j] - 0.6*D
    
    nstep = 0       
    
    # Time steps       
    while nstep * dt < timesteps:
        nstep = nstep + 1           
                   
        # Reflective boundary conditions         
        H[:,0] = H[:,1];      U[:,0] = U[:,1];       V[:,0] = -V[:,1];
        H[:,m+1] = H[:,m];  U[:,m+1] = U[:,m];     V[:,m+1] = -V[:,m];        
        H[0,:] = H[1,:];     U[0,:] = -U[1,:];        V[0,:] = V[1,:];         
        H[m+1,:] = H[m,:]; U[m+1,:] = -U[m,:];      V[m+1,:] = V[m,:];           
        
        # First half step             
        
        # x direction        
        i = np.s_[0:m+1];     
        j = np.s_[0:m];             
        
        ip1 = np.s_[1:m+2]
        jp1 = np.s_[1:m+1]
        
        # height        
        Hx[i,j] = (H[ip1,jp1]+H[i,jp1])/2 - dt/(2*dx)*(U[ip1,jp1]-U[i,jp1]);             
        
        # x momentum     
        Ux[i,j] = (U[ip1,jp1]+U[i,jp1])/2 - dt/(2*dx)*((U[ip1,jp1]**2/H[ip1,jp1] + g/2*H[ip1,jp1]**2) - (U[i,jp1]**2/H[i,jp1] + g/2*H[i,jp1]**2));             
        
        # y momentum         
        Vx[i,j] = (V[ip1,jp1]+V[i,jp1])/2 - dt/(2*dx)*((U[ip1,jp1]*V[ip1,jp1]/H[ip1,jp1]) - (U[i,jp1]*V[i,jp1]/H[i,jp1]));                 
        
        # y direction        
        tmp = i; i = j; j = tmp;
        tmp = ip1; ip1 = jp1; jp1 = tmp;       
        
        # height        
        Hy[i,j] = (H[ip1,jp1]+H[ip1,j])/2 - dt/(2*dy)*(V[ip1,jp1]-V[ip1,j]);             
        
        # x momentum         
        Uy[i,j] = (U[ip1,jp1]+U[ip1,j])/2 - dt/(2*dy)*((V[ip1,jp1]*U[ip1,jp1]/H[ip1,jp1]) - (V[ip1,j]*U[ip1,j]/H[ip1,j]));        
        
        # y momentum         
        Vy[i,j] = (V[ip1,jp1]+V[ip1,j])/2 - dt/(2*dy)*((V[ip1,jp1]**2/H[ip1,jp1] + g/2*H[ip1,jp1]**2) - (V[ip1,j]**2/H[ip1,j] + g/2*H[ip1,j]**2));             
        
        # Second half step        
        i = j = np.s_[1:m+1];                  
        im1 = jm1 = np.s_[0:m];
        
        # height         
        H[i,j] = H[i,j] - (dt/dx)*(Ux[i,jm1]-Ux[im1,jm1]) - (dt/dy)*(Vy[im1,j]-Vy[im1,jm1]);        
        
        # x momentum         
        U[i,j] = U[i,j] - (dt/dx)*((Ux[i,jm1]**2/Hx[i,jm1] + g/2*Hx[i,jm1]**2) -  (Ux[im1,jm1]**2/Hx[im1,jm1] + g/2*Hx[im1,jm1]**2)) \
                        - (dt/dy)*((Vy[im1,j]*Uy[im1,j]/Hy[im1,j]) - (Vy[im1,jm1]*Uy[im1,jm1]/Hy[im1,jm1]));
        
        # y momentum         
        V[i,j] = V[i,j] - (dt/dx)*((Ux[i,jm1]*Vx[i,jm1]/Hx[i,jm1]) - (Ux[im1,jm1]*Vx[im1,jm1]/Hx[im1,jm1])) \
                        - (dt/dy)*((Vy[im1,j]**2/Hy[im1,j] + g/2*Hy[im1,j]**2) - (Vy[im1,jm1]**2/Hy[im1,jm1] + g/2*Hy[im1,jm1]**2));             
        
        #C = np.abs(U[i,j]) + np.abs(V[i,j]);  # Color shows momemtum             
        #if nstep % 100 == 0:
        np.savetxt("data/data%s"%(nstep), H, fmt=['%f']*H.shape[1])
        
        #if np.all(np.all(np.isnan(H))): #Unstable, restart    
        #    break

if __name__ == "__main__":
    genData()
