package net.deepstorage.compscan.generator;

import java.io.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.text.*;
import java.util.*;

import net.deepstorage.compscan.Compressor;
import net.deepstorage.compscan.generator.gui.*;

public class Gui extends JFrame{
   private final static String[] compressors={
      "None", "LZ4", "GZIP", "LZW"
   };
   private final static Integer[][] compressorsOptions=new Integer[][]{
      {}, range(1,17), range(1,9), {},
   };
   static Integer[] range(int a, int b){
      Integer[] range=new Integer[b-a+1];
      for(int v=a; v<=b; v++) range[v-a]=v;
      return range;
   }
   
   private final int CP_NONE=0;
   private final int CP_LZ4=1;
   private final int CP_GZIP=2;
   private final int CP_LZW=3;
   
   private final App app=new App();
   
   private final NumberField blockSizeField=new NumberField.Natural(10);
   private final OkLabel blockSizeOk=new OkLabel();
   private final JComboBox blockSizeUnits=new JComboBox(new String[]{
      "B",
      "KiB",
      "MiB",
   });
   private final int[] blockSizeUnitValues={
      1, 1024, 1024*1024
   };
   private final NumberField superblockSizeField=new NumberField.Natural(5);
   
   private final HashesDefPanel hashesDefPanel=new HashesDefPanel(app, blockSizeField);
   
   private final NumberField cpRatioField=new NumberField.Ratio(10);
   private final OkLabel cpRatioOk=new OkLabel();
   private final JComboBox cpMethod=new JComboBox(compressors);
   private final JComboBox cpOption=new JComboBox();
   
   private final JEditorPane outputPathField=new JEditorPane();
   private final OkLabel outputPathOk=new OkLabel();
   private final JCheckBox createOutput=new JCheckBox("Create missing dirs", true);
   private final JCheckBox overwriteOutput=new JCheckBox("Overwrite existing files", false);
   private final JButton outputBrowseBtn=new JButton("Browse");
   {
      outputPathField.setBorder(blockSizeField.getBorder());
   }
   
   public JTextField prefixField=new JTextField(20);
   private final OkLabel prefixOk=new OkLabel();
   private final NumberField fcField=new NumberField.Natural(10);
   private final NumberField fcOffField=new NumberField.Natural(10);
   
   private final JTextArea console=new JTextArea();
   private final JScrollPane scrollPane = new JScrollPane(console);
   {
      scrollPane.setPreferredSize(new Dimension(500,150));

      console.setBorder(cpRatioField.getBorder());
      console.setEditable(false);
      DefaultCaret caret = (DefaultCaret)console.getCaret();
      caret.setUpdatePolicy(DefaultCaret.ALWAYS_UPDATE);
   }
   private final JProgressBar progBar=new JProgressBar(
      SwingConstants.HORIZONTAL, 0, 100
   );
   
   private final JButton start=new JButton("Start");
   
   private boolean running;
   
   public Gui(){
      getContentPane().setLayout(new GridLayout(1,1));
      getContentPane().add(UiUtil.makeVBox(new float[]{0,0,0,1,0,0,0,0}, 5, new Component[]{
         UiUtil.titledBox("Generation", buildGenerationPanel()),
         UiUtil.titledBox("Output", buildOutputPanel()),
         UiUtil.vgap(5),
         scrollPane,
         UiUtil.vgap(2),
         progBar,
         UiUtil.vgap(5),
         UiUtil.makeHBox(1,0,0, 5, new Component[]{
            UiUtil.hglue(), start
         })
      }));
      bind(cpRatioField, cpRatioOk);
      bind(blockSizeField, blockSizeOk);
      bind(prefixField, prefixOk);
      bindCompressionOptions();
      bindOutputPath();
      
      start.addActionListener(e->{
         start();
      });
   }
   
   private void start(){
      console.setText("");
      try{
         PrintWriter log=new PrintWriter(new FileWriter("generator.log"));
         try{
            app.reset();
            setup();
            new Thread(()->{
               run(log);
            }).start();
         }
         catch(Exception e){
            UiUtil.errMsg(this, "Starting", e.getMessage());
            e.printStackTrace(log);
e.printStackTrace();
         }
         finally{
            log.close();
         }
      }
      catch(Exception e){
e.printStackTrace();
         UiUtil.errMsg(this, "Opening log file", e.getMessage());
      }
   }
   
   private void run(PrintWriter log){
      App.Stage stage=app.start();
      while(stage!=null){
         try{
            println(stage.name());
            setProgress(0);
            while(stage.hasMoreSteps()){
               stage.step();
               if(stage.messages.size()>0){
                  for(String msg: stage.messages) println(msg);
                  stage.messages.clear();
               }
               setProgress(stage.progress());
            }
            stage=stage.nextStage();
         }
         catch(App.BadParam err){
            println("Error in configuration: "+err.getMessage());
            return;
         }
         catch(Exception e){
            println("Unexpected error: "+e.getMessage());
            e.printStackTrace(log);
e.printStackTrace();
            return;
         }
      }
      println("done");
   }
   
   private void println(String s){
      SwingUtilities.invokeLater(()->{
         Document doc=console.getDocument();
         try{
            doc.insertString(doc.getLength(), s+"\n", null);
         }
         catch(Exception e){}
      });
   }
   
   private void setProgress(float f){
      SwingUtilities.invokeLater(()->{
         progBar.setValue((int)(f*100));
      });
   }
   
   private void setup() throws Exception{
      app.hashesDefSupplier=hashesDefPanel.getValue();
      app.compressor=Compressor.getCompressionInterface(getCompressorString());
      app.compressionRatio= cpRatioField.currentValue==null? 
         null: cpRatioField.currentValue.floatValue()
      ;
      app.blockSize=getBlockSize();
      app.superblockSize= superblockSizeField.currentValue==null? 
         null: superblockSizeField.currentValue.intValue()
      ;
      app.outputPath=outputPathField.getText();
      app.createMissingDir=createOutput.isSelected();
      app.overrideOutput=overwriteOutput.isSelected();
      app.filenamePrefix=prefixField.getText();
      app.fileCount= fcField.currentValue==null? 
         null: fcField.currentValue.intValue()
      ;
      app.indexOffset= fcOffField.currentValue==null? 
         null: fcOffField.currentValue.intValue()
      ;
   }
   
   private String getCompressorString(){
      String method=(String)cpMethod.getSelectedItem();
      Integer opt=(Integer)cpOption.getSelectedItem();
      if(opt!=null) method+=":"+opt;
      return method;
   }
   
   private Integer getBlockSize(){
      if(blockSizeField.currentValue==null) return null;
      int unit=blockSizeUnitValues[blockSizeUnits.getSelectedIndex()];
      return blockSizeField.currentValue.intValue()*unit;
   }
   
   private Component buildGenerationPanel(){
      return UiUtil.makeVBox(0,0,1, 0, new Component[]{
         UiUtil.titledBox("Hashes", hashesDefPanel),
         UiUtil.makeHBox(0,0,1, 0, new Component[]{
            UiUtil.makeGrid(
               new float[]{0,1}, new float[]{1,1}, true, false, 5, 3, 
               new Component[][]{
                  {
                     UiUtil.makeHBox(1,0,0, 5, new Component[]{
                        UiUtil.hglue(), new JLabel("Compression ratio"), UiUtil.hgap(20)
                     }),
                     UiUtil.makeHBox(0,0,1, 5, new Component[]{
                        cpRatioField, UiUtil.hgap(3), cpRatioOk, UiUtil.hglue()
                     }),
                  },
                  {
                     UiUtil.makeHBox(1,0,0, 2, new Component[]{
                        UiUtil.hglue(), new JLabel("Compression method"), UiUtil.hgap(20)
                     }),
                     UiUtil.makeHBox(0,0,1, 2, new Component[]{
                        cpMethod, UiUtil.hgap(3), cpOption, UiUtil.hglue()
                     }),
                  },
               }
            ),
            UiUtil.hgap(30),
            UiUtil.makeGrid(
               new float[]{0,1}, new float[]{1,1,1,1}, true, false, 5, 3, 
               new Component[][]{
                  {
                     UiUtil.makeHBox(1,0,0, 2, new Component[]{
                        UiUtil.hglue(), new JLabel("Block size"), UiUtil.hgap(20)
                     }),
                     UiUtil.makeHBox(0,0,1, 2, new Component[]{
                        blockSizeField, UiUtil.hgap(1), blockSizeUnits, UiUtil.hgap(3), blockSizeOk, UiUtil.hglue()
                     }),
                  },
                  {
                     UiUtil.makeHBox(1,0,0, 2, new Component[]{
                        UiUtil.hglue(), new JLabel("Superblock size"), UiUtil.hgap(20)
                     }),
                     UiUtil.makeHBox(0,0,1, 2, new Component[]{
                        superblockSizeField, UiUtil.hgap(10), new JLabel("blocks"), UiUtil.hglue()
                     }),
                  }
               }
            ),
         }),
         UiUtil.vglue()
      });
   }
   
   private Component buildOutputPanel(){
      return UiUtil.makeVBox(0,1,0, 5, new Component[]{
         outputPathField,
         UiUtil.makeHBox(0,1,0, 5, new Component[]{
            UiUtil.makeHBox(0,0,0, 0, new Component[]{
               createOutput, UiUtil.hgap(30), overwriteOutput
            }),
            UiUtil.hgap(40),
            UiUtil.makeHBox(0,0,0, 0, new Component[]{
               outputPathOk, UiUtil.hgap(3), outputBrowseBtn
            }),
         }),
         UiUtil.makeGrid(
            new float[]{0,1}, new float[]{0,0,0}, true, false,3,3,
            new Component[][]{
               {
                  UiUtil.makeHBox(1,0,0, 0, new Component[]{
                     UiUtil.hglue(), new JLabel("File prefix")
                  }),
                  UiUtil.makeHBox(0,0,1, 0, new Component[]{
                     prefixField, UiUtil.hgap(3), prefixOk, UiUtil.hglue()
                  }),
               },
               {
                  UiUtil.makeHBox(1,0,0, 0, new Component[]{
                     UiUtil.hglue(), new JLabel("File count")
                  }),
                  UiUtil.makeHBox(0,0,1, 0, new Component[]{
                     fcField, UiUtil.hglue()
                  }),
               },
               {
                  UiUtil.makeHBox(1,0,0, 0, new Component[]{
                     UiUtil.hglue(), new JLabel("File numbering offset")
                  }),
                  UiUtil.makeHBox(0,0,1, 0, new Component[]{
                     fcOffField, UiUtil.hglue()
                  }),
               },
            }
         )
      });
   }
   
   private void bind(NumberField nf, OkLabel ok){
      nf.listeners.add(v->ok.setValue(v!=null));
   }
   
   private void bind(JTextField tf, OkLabel ok){
      tf.getDocument().addDocumentListener(
         UiUtil.onAnyDocumentChange(()->{
            String s=tf.getText();
            ok.setValue(s!=null && s.length()>0);
         })
      );
   }
   
   private void bindCompressionOptions(){
      cpMethod.addActionListener(e->{
         Integer[] opts=compressorsOptions[cpMethod.getSelectedIndex()];
         cpOption.setModel(new DefaultComboBoxModel(opts));
      });
   }
   
   private void bindOutputPath(){
      outputBrowseBtn.addActionListener(e->{
         File dir=new File(outputPathField.getText());
         if(!dir.exists()) dir=null;
         dir=UiUtil.requestDir("Select output directory", dir);
         outputPathField.setText(dir.getAbsolutePath());
      });
      outputPathField.getDocument().addDocumentListener(
         UiUtil.onAnyDocumentChange(()->{
            File f=new File(outputPathField.getText());
            outputPathOk.setValue(f.exists() && f.isDirectory());
         })
      );
   }
   
   
      
   public static void main(String[] args) {
      //System.out.println(Arrays.asList(range(6,15)));
      
      Gui gui=new Gui();
      gui.pack();
      gui.show();

//      JFrame frame=new JFrame("test");
//      //frame.getContentPane().add(gui.buildOutputPanel());
//      //frame.getContentPane().add(gui.buildGenerationPanel());
//      frame.pack();
//      frame.show();
   }   
}
